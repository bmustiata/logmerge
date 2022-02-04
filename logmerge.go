package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

type FileLine struct {
	fileName string
	text     string
}

type FileRecord struct {
	timestamp int64
	fileName  string
	content   string
}

type FilterTimeWindow struct {
	startTimestamp *int64
	endTimestamp *int64
}

type TestOnlyFlags struct {
	currentTime *int64
}

type AppConfig struct {
	testOnly TestOnlyFlags
	window FilterTimeWindow
	filesToMix     []string
	outputFileName string
	channelSize int
}

var FILE_RECORD_RE *regexp.Regexp;
var STDIN_READER *bufio.Reader

func init() {
	FILE_RECORD_RE = regexp.MustCompile(`^(\d+/\d+\.\d+)\s`)

	// we need to reuse the reader, since it's buffered
	STDIN_READER = bufio.NewReader(os.Stdin)
}

// The processing works the following way:
//     toRecord            orderByTime             filter             writeLog
// f1.txt --> chan FileRecord --+--> chan FileRecord --> chan FileRecord --> out.txt
// f2.txt --> chan FileRecord --+
// f3.txt --> chan FileRecord --+

func main() {
	config := readApplicationConfig()

	lineChannels := toLineChannels(config, config.filesToMix)
	logRecordsChannels := toRecords(config, lineChannels)
	orderedRecordChannel := orderByTime(config, logRecordsChannels)
	filteredRecordChannel := filter(config, orderedRecordChannel)

	writeLog(config.outputFileName, filteredRecordChannel)
}

func readApplicationConfig() AppConfig {
	var isWindow bool
	var windowStartTimeString, windowEndTimeString, outputFileName string
	var testOnlyCurrentTime string
	var channelSize int

	flag.BoolVar(&isWindow, "w", false, "Use a time window to filter records")
	flag.StringVar(&windowStartTimeString, "window-start", "", "Start time to filter log entries")
	flag.StringVar(&windowEndTimeString, "window-end", "", "End time to filter log entries")
	flag.StringVar(&outputFileName, "output", "", "The output file to write")
	flag.StringVar(&testOnlyCurrentTime, "test-only-current-time", "", "DO NOT USE")
	flag.IntVar(&channelSize, "channel-size", 10, "How big to make the channels (buffering)")

	flag.Parse()

	result := AppConfig{
		filesToMix:     flag.Args(),
		outputFileName: outputFileName,
		channelSize: channelSize,
	}

	if testOnlyCurrentTime != "" {
		fakeCurrentTime := MustParseTime(testOnlyCurrentTime, "2006.01.02 15:04")
		result.testOnly.currentTime = &fakeCurrentTime
	}

	if isWindow || windowStartTimeString != "" || windowEndTimeString != "" {
		result.window = createTimeWindowFilter(result, windowStartTimeString, windowEndTimeString)
	}

	return result
}

func createTimeWindowFilter(config AppConfig, windowStart string, windowEnd string) FilterTimeWindow {
	window := FilterTimeWindow{}
	utcnow := GetCurrentTime(config)

	if windowStart == "" {
		windowStart = readFromUser("window start time (yyyy.MM.dd hh:mm  /  hh:mm  /  n/now):")
	}

	window.startTimestamp = parseTimestampValue(windowStart, utcnow)

	if window.startTimestamp != nil &&
			*window.startTimestamp > utcnow &&
			windowStart != "now" {
		*window.startTimestamp -= 3600 * 24
	}

	if windowEnd == "" {
		windowEnd = readFromUser("window end time (yyyy.MM.dd hh:mm  /  hh:mm  /  n/now):")
	}

	window.endTimestamp = parseTimestampValue(windowEnd, utcnow)

	if window.endTimestamp != nil &&
			*window.endTimestamp > utcnow &&
			windowEnd != "now" {
		*window.endTimestamp -= 3600 * 24
	}

	// end window times should finish at the end of the minute
	if window.endTimestamp != nil {
		*window.endTimestamp += 60000 - 1
	}

	return window
}

func GetCurrentTime(config AppConfig) int64 {
	if config.testOnly.currentTime != nil {
		return *config.testOnly.currentTime
	}

	return time.Now().UnixMilli()
}

func parseTimestampValue(timeString string, utcnow int64) *int64 {
	if timeString == "" {
		return nil
	}

	if timeString == "now" || timeString == "n" {
		return &utcnow
	}


	if strings.ContainsRune(timeString, ' ') {
		result := MustParseTime(timeString, "2006.01.02 15:04")
		return &result
	}

	result := MustParseTime(timeString, "15:04")

	var dayInfo int64 = 1000 * 3600 * 24
	result = utcnow - utcnow % dayInfo + result % dayInfo

	return &result
}

func MustParseTime(timeString, format string) int64 {
	t, err := time.Parse(format, timeString)

	if err != nil {
		log.Fatal(fmt.Errorf("unable to parse %s with format %s: %w",
			timeString,
			format,
			err,
		))
	}

	return t.UnixMilli()
}

func readFromUser(label string) string {
	fmt.Fprint(os.Stderr, label+" ")
	s, _ := STDIN_READER.ReadString('\n')

	return strings.TrimSpace(s)
}

// Convert the given slice of file names to a slice of channels
// that yield individual lines.
func toLineChannels(config AppConfig, files []string) []chan FileLine {
	result := make([]chan FileLine, len(files))

	for i, file := range files {
		c := make(chan FileLine, config.channelSize)
		go readFileLines(file, c)

		result[i] = c
	}

	return result
}

func toRecords(config AppConfig, lineChannels []chan FileLine) []chan FileRecord {
	result := make([]chan FileRecord, len(lineChannels))

	for i, lineChannel := range lineChannels {
		result[i] = readMultilineLogEntry(config, lineChannel)
	}

	return result
}

// orderByTime Reads all the channels and returns the next row in order
func orderByTime(config AppConfig, channels []chan FileRecord) chan FileRecord {
	result := make(chan FileRecord, config.channelSize)

	go func() {
		defer close(result)

		activeChannels := make(map[chan FileRecord]bool)
		activeChannelsLastValues := make(map[chan FileRecord]FileRecord)
		channelIndex := make(map[chan FileRecord]int)

		for index, channel := range channels {
			channelIndex[channel] = index
			activeChannels[channel] = true
			readNextValueOrRemove(channel, activeChannels, activeChannelsLastValues)
		}

		for len(activeChannels) > 0 {
			newestRecord, channel := findNewestRecord(activeChannelsLastValues, channelIndex)
			result <- newestRecord
			readNextValueOrRemove(channel, activeChannels, activeChannelsLastValues)
		}
	}()

	return result
}

func readNextValueOrRemove(
	channel chan FileRecord,
	channels map[chan FileRecord]bool,
	channelLastValue map[chan FileRecord]FileRecord) {
	value, ok := <- channel

	if ! ok {
		delete(channels, channel)
		delete(channelLastValue, channel)
		return
	}

	channelLastValue[channel] = value
}

func findNewestRecord(values map[chan FileRecord]FileRecord,
					  channelIndex map[chan FileRecord]int) (FileRecord, chan FileRecord) {
	type Pair struct {
		channel chan FileRecord
		record FileRecord
	}

	records := make([]Pair, 0, len(values))

	for k, v := range values {
		records = append(records, Pair{
			channel: k,
			record: v,
		})
	}

	sort.SliceStable(records, func(i, j int) bool {
		// When multiple lines are matching at the same millisecond, we need to ensure
		// we cluster them by the files order we received.
		if records[i].record.timestamp == records[j].record.timestamp {
			return channelIndex[records[i].channel] < channelIndex[records[j].channel]
		}

		return records[i].record.timestamp < records[j].record.timestamp
	})

	return records[0].record, records[0].channel
}

// filter only the entries that are in the specified time window
func filter(config AppConfig, input chan FileRecord) chan FileRecord {
	output := make(chan FileRecord, config.channelSize)

	// FIXME: when the record exit the window bounds, we should just close the input stream
	go func() {
		defer close(output)

		record, ok := <-input

		for ok {
			if isRecordValid(config, record) {
				output <- record
			}

			record, ok = <-input
		}
	}()

	return output
}

func readFileLines(inputFileName string, output chan FileLine) {
	defer close(output)
	f, err := os.Open(inputFileName)

	if err!= nil {
		log.Fatal(fmt.Errorf("unable to open %s: %w", inputFileName, err))
	}

	s := bufio.NewScanner(f)

	for s.Scan() {
		output <- FileLine{
			fileName: inputFileName,
			text:     s.Text(),
		}
	}

	if s.Err() != nil {
		log.Fatal(fmt.Errorf("unable to read %s: %w", inputFileName, s.Err()))
	}
}

// readMultilineLogEntry Reads lines from the log firing multiline records
// The multiline records will have also the parsed timestamp when they
// were created.
func readMultilineLogEntry(config AppConfig, input chan FileLine) chan FileRecord {
	output := make(chan FileRecord, config.channelSize)

	go func() {
		defer close(output)

		var entry FileRecord
		var line FileLine
		var ok bool

		for {
			line, ok = <-input

			if ! ok {
				break
			}

			isNewRecord, ts := isLineNewRecord(line.text)

			if !isNewRecord {
				continue
			}

			entry = FileRecord{
				timestamp: ts,
				fileName:  line.fileName,
				content:   line.text,
			}

			break
		}

		if !ok {
			return
		}

		for {
			line, ok = <-input

			if !ok {
				break
			}

			isNewRecord, ts := isLineNewRecord(line.text)

			if !isNewRecord {
				entry.content += "\n" + line.text
				continue
			}

			output <- entry

			entry = FileRecord{
				timestamp: ts,
				fileName:  line.fileName,
				content:   line.text,
			}
		}

		// write the last entry
		output <- entry
	}()

	return output
}

func isRecordValid(config AppConfig, record FileRecord) bool {
	if config.window.startTimestamp != nil && record.timestamp < *config.window.startTimestamp {
		return false
	}

	if config.window.endTimestamp != nil && record.timestamp > *config.window.endTimestamp {
		return false
	}

	return true
}

// writeLog write all the entries from input into the specified file
func writeLog(outFileName string, input chan FileRecord) {
	f, err := os.Create(outFileName)

	if err != nil {
		log.Fatal(fmt.Errorf("unable to create outputFileName file %s: %w", outFileName, err))
	}

	r := bufio.NewWriter(f)

	record, ok := <-input

	for ok {
		basePath := path.Base(record.fileName)
		_, err = r.WriteString(basePath + " " + record.content + "\n")

		if err != nil {
			log.Fatal(fmt.Errorf("unable to write into outputFileName file %s: %w", outFileName, err))
		}

		record, ok = <-input
	}

	err = r.Flush()

	if err != nil {
		log.Fatal(fmt.Errorf("unable to flush outputFileName %s: %w", outFileName, err))
	}

	f.Close()
}

func isLineNewRecord(line string) (bool, int64) {
	m := FILE_RECORD_RE.FindStringSubmatch(line)

	if m == nil {
		return false, -1
	}

	timestamp, err := parseTimestamp(m[1])

	if err != nil {
		return false, -2
	}

	return true, timestamp
}

func parseTimestamp(stringTimestamp string) (int64, error) {
	parse, err := time.Parse("20060102/150405.000", stringTimestamp)

	if err != nil {
		return -1, err
	}

	return parse.UnixMilli(), nil
}
