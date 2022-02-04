package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"time"
)

type FileLine struct {
	fileName string
	line     string
}

type FileRecord struct {
	timestamp int64
	fileName  string
	content   string
}

var FILE_RECORD_RE *regexp.Regexp;

func init() {
	FILE_RECORD_RE = regexp.MustCompile(`^(\d+/\d+\.\d+)\s`)
}

func main() {
	files := []string {
		"features/steps/test_data/file1.txt",
		"features/steps/test_data/file2.txt",
		"features/steps/test_data/multiline.txt",
	}
	lineChannels := createLineChannels(files)
	logEntriesChannels := createLogEntriesChannels(lineChannels)
	orderedRecordChannel := createOrderByTimeChannel(logEntriesChannels)
	fiteredResultsChannel := filterLogRecord(orderedRecordChannel)
	writeLog("/tmp/out.txt", fiteredResultsChannel)
}

func createLineChannels(files []string) []chan FileLine {
	result := make([]chan FileLine, len(files))

	for i, file := range files {
		c := make(chan FileLine)
		go readFileLines(file, c)

		result[i] = c
	}

	return result
}

func createLogEntriesChannels(lineChannels []chan FileLine) []chan FileRecord {
	result := make([]chan FileRecord, len(lineChannels))

	for i, lineChannel := range lineChannels {
		result[i] = readMultilineLogEntry(lineChannel)
	}

	return result
}

// createOrderByTimeChannel Reads all the channels and returns the next row in order
func createOrderByTimeChannel(channels []chan FileRecord) chan FileRecord {
	result := make(chan FileRecord)

	go func() {
		defer close(result)

		activeChannels := make(map[chan FileRecord]bool)
		activeChannelsLastValues := make(map[chan FileRecord]FileRecord)

		for _, channel := range channels {
			activeChannels[channel] = true
			readNextValueOrRemove(channel, activeChannels, activeChannelsLastValues)
		}

		for len(activeChannels) > 0 {
			newestRecord, channel := findNewestRecord(activeChannelsLastValues)
			result <- newestRecord
			readNextValueOrRemove(channel, activeChannels, activeChannelsLastValues)
		}
	}()

	return result
}

func findNewestRecord(values map[chan FileRecord]FileRecord) (FileRecord, chan FileRecord) {
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

	sort.Slice(records, func(i, j int) bool {
		return records[i].record.timestamp < records[j].record.timestamp
	})

	return records[0].record, records[0].channel
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

func filterLogRecord(input chan FileRecord) chan FileRecord {
	output := make(chan FileRecord)

	// FIXME: when the record exit the window bounds, we should just close the input stream
	go func() {
		record, ok := <-input

		for ok {
			if isRecordValid(record) {
				output <- record
			}

			record, ok = <-input
		}

		defer close(output)
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
			line: s.Text(),
		}
	}

	if s.Err() != nil {
		log.Fatal(fmt.Errorf("unable to read %s: %w", inputFileName, s.Err()))
	}
}

// readMultilineLogEntry Reads lines from the log firing multiline records
func readMultilineLogEntry(input chan FileLine) chan FileRecord {
	output := make(chan FileRecord)

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

			isNewEntry, ts := readNewEntry(line.line)

			if !isNewEntry {
				continue
			}

			entry = FileRecord{
				timestamp: ts,
				fileName:  line.fileName,
				content:   line.line,
			}

			break;
		}

		if !ok {
			return
		}

		for {
			line, ok = <-input

			if !ok {
				break
			}

			isNewEntry, ts := readNewEntry(line.line)

			if !isNewEntry {
				entry.content += "\n" + line.line
				continue
			}

			output <- entry

			entry = FileRecord{
				timestamp: ts,
				fileName:  line.fileName,
				content:   line.line,
			}
		}

		// write the last entry
		output <- entry
	}()

	return output
}

func isRecordValid(record FileRecord) bool {
	// FIXME: implement
	return true
}

// writeLog write all the entries from input into the specified file
func writeLog(outFileName string, input chan FileRecord) {
	f, err := os.Create(outFileName)

	if err != nil {
		log.Fatal(fmt.Errorf("unable to create output file %s: %w", outFileName, err))
	}

	r := bufio.NewWriter(f)

	record, ok := <-input

	for ok {
		_, err = r.WriteString(record.content + "\n")

		if err != nil {
			log.Fatal(fmt.Errorf("unable to write into output file %s: %w", outFileName, err))
		}

		record, ok = <-input
	}

	err = r.Flush()

	if err != nil {
		log.Fatal(fmt.Errorf("unable to flush output %s: %w", outFileName, err))
	}

	f.Close()
}

func readNewEntry(line string) (bool, int64) {
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
