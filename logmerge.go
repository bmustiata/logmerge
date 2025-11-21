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
	timestamp time.Time
	fileName  string
	content   string
}

type FilterTimeWindow struct {
	startTimestamp *time.Time
	endTimestamp *time.Time
}

type TestOnlyFlags struct {
	currentTime *time.Time
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

const ONE_DAY_MILLIS = 3600 * 24 * 1000

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
	flag.IntVar(&channelSize, "channel-size", 10000, "How big to make the channels (buffering)")

	flag.Parse()

	result := AppConfig{
		filesToMix:     flag.Args(),
		outputFileName: outputFileName,
		channelSize: channelSize,
	}

	if testOnlyCurrentTime != "" {
		fakeCurrentTime, _ := MustParseTime(time.Now(), testOnlyCurrentTime)
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
	hasSeconds := false

	if windowStart == "" {
		windowStart = readFromUser("window start time: yyyy.MM.dd hh:mm(:ss) | hh:mm(:ss) | n/now>")
	}

	window.startTimestamp, hasSeconds = parseTimestampValue(windowStart, utcnow)

	if window.startTimestamp != nil &&
			window.startTimestamp.After(utcnow) &&
			windowStart != "now" {
		*window.startTimestamp = window.startTimestamp.Add(time.Hour * -24)
	}

	if windowEnd == "" {
		windowEnd = readFromUser("window end time (yyyy.MM.dd hh:mm(:ss) | hh:mm(:ss) | n/now)>")
	}

	window.endTimestamp, hasSeconds = parseTimestampValue(windowEnd, utcnow)

	// end window times should finish at the end of the minute/second
	if window.endTimestamp != nil {
		// if our time contains seconds, it should be end of the second
		if hasSeconds {
			*window.endTimestamp = fillMillisTo999(window.endTimestamp)
		} else {
			*window.endTimestamp = fillSecMillisTo999(window.endTimestamp)
		}
	}

	if window.endTimestamp != nil &&
		window.endTimestamp.After(utcnow) &&
		windowEnd != "now" {
		*window.endTimestamp = window.endTimestamp.Add(time.Hour * -24)
	}

	// End window times should be after the start times. This can happen if the
	// user entered local times and the day changed:
	// startTimestamp: 23:50, endTimestamp: 01:30
	if window.endTimestamp != nil && window.startTimestamp != nil &&
		window.endTimestamp.Before(*window.startTimestamp) {
		*window.endTimestamp = window.endTimestamp.Add(time.Hour * 24)
	}

	return window
}

func fillSecMillisTo999(timestamp *time.Time) time.Time {
	return time.Date(
		timestamp.Year(),
		timestamp.Month(),
		timestamp.Day(),
		timestamp.Hour(),
		timestamp.Minute(),
		59,
		int(1 * time.Second - 1 * time.Microsecond),
		time.Local,
	)
}

func fillMillisTo999(timestamp *time.Time) time.Time {
	return time.Date(
		timestamp.Year(),
		timestamp.Month(),
		timestamp.Day(),
		timestamp.Hour(),
		timestamp.Minute(),
		timestamp.Second(),
		int(1 * time.Second - 1 * time.Microsecond),
		time.Local,
	)
}

func GetCurrentTime(config AppConfig) time.Time {
	if config.testOnly.currentTime != nil {
		return *config.testOnly.currentTime
	}

	return time.Now()
}

func parseTimestampValue(timeString string, now time.Time) (*time.Time, bool) {
	if timeString == "" {
		return nil, false
	}

	if timeString == "now" || timeString == "n" {
		return &now, true
	}

	result, hasSeconds := MustParseTime(now, timeString)

	return &result, hasSeconds
}

func MustParseTime(now time.Time, timeString string) (time.Time, bool) {
	type TimeStruct struct {
		format string
		hasSeconds bool
		hasYear bool
	}

	timeLayouts := []TimeStruct{
		{format: "2006.01.02 15:04", hasSeconds: false, hasYear: true},
		{format: "2006.01.02 15:04:05", hasSeconds: true, hasYear: true},
		{format: "15:04", hasSeconds: false},
		{format: "15:04:05", hasSeconds: true},
	}

	for _, timeLayout := range timeLayouts {
		// FIXME: make both the local time, and the log time zone configurable, and prefer the
		//        log timezone. Unless stated, we assume the times in the log are in the local
		//        timezone.
		t, err := time.ParseInLocation(timeLayout.format, timeString, time.Local);

		if err != nil {
			continue
		}

		if timeLayout.hasYear {
			return t, timeLayout.hasSeconds
		}

		r := time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.Local)

		return r, timeLayout.hasSeconds
	}

	log.Fatal(fmt.Errorf("unable to parse time string: %s", timeString))
	return time.Time{}, false // => not reached
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

		return records[i].record.timestamp.Before(records[j].record.timestamp)
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
				timestamp: *ts,
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
				timestamp: *ts,
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
	if config.window.startTimestamp != nil && record.timestamp.Before(*config.window.startTimestamp) {
		return false
	}

	if config.window.endTimestamp != nil && record.timestamp.After(*config.window.endTimestamp) {
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
		recordLines := strings.Split(record.content, "\n")

		for _, line := range recordLines {
			_, err = r.WriteString(basePath + " " + line + "\n")
		}

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

func isLineNewRecord(line string) (bool, *time.Time) {
	m := FILE_RECORD_RE.FindStringSubmatch(line)

	if m == nil {
		return false, nil
	}

	timestamp, err := parseTimestamp(m[1])

	if err != nil {
		return false, nil
	}

	return true, timestamp
}

func parseTimestamp(stringTimestamp string) (*time.Time, error) {
	t, err := time.ParseInLocation("20060102/150405.000", stringTimestamp, time.Local)

	if err != nil {
		return nil, err
	}

	return &t, nil
}
