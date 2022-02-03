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

type FileRecord struct {
	timestamp int64
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
	multilineLogEntryChannels := createContentChannels(lineChannels)
	recordChannels := createRecordChannels(multilineLogEntryChannels)
	orderedRecordChannel := createOrderByTimeChannel(recordChannels)
	fiteredResultsChannel := filterLogRecord(orderedRecordChannel)
	writeLog("/tmp/out.txt", fiteredResultsChannel)
}

func createLineChannels(files []string) []chan string {
	result := make([]chan string, len(files))

	for i, file := range files {
		c := make(chan string)
		go readFileLines(file, c)

		result[i] = c
	}

	return result
}

func createContentChannels(lineChannels []chan string) []chan string {
	result := make([]chan string, len(lineChannels))

	for i, lineChannel := range lineChannels {
		c := make(chan string)
		go readMultilineLogEntry(lineChannel, c)

		result[i] = c
	}

	return result
}

func createRecordChannels(contentChannels []chan string) []chan FileRecord {
	result := make([]chan FileRecord, len(contentChannels))

	for i, file := range contentChannels {
		c := make(chan FileRecord)
		go convertLogEntryToRecord(file, c)

		result[i] = c
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

func readFileLines(inputFileName string, output chan string) {
	defer close(output)
	f, err := os.Open(inputFileName)

	if err!= nil {
		log.Fatal(fmt.Errorf("unable to open %s: %w", inputFileName, err))
	}

	s := bufio.NewScanner(f)

	for s.Scan() {
		output <- s.Text()
	}

	if s.Err() != nil {
		log.Fatal(fmt.Errorf("unable to read %s: %w", inputFileName, s.Err()))
	}
}

// readMultilineLogEntry Reads lines from the log firing multiline records
func readMultilineLogEntry(input chan string, output chan string) {
	defer close(output)

	var content, line string
	content, ok := <-input

	if !ok {
		return
	}

	for {
		line, ok = <-input

		if !ok {
			break
		}

		if !isNewRecord(line) {
			content += line
		}

		output <- content
		content = line
	}
}

func convertLogEntryToRecord(input chan string, output chan FileRecord) {
	defer close(output)

	line, ok := <-input

	for ok {
		ts, _ := parseTimestamp(line)
		output <- FileRecord{
			content:   line,
			timestamp: ts,
		}

		line, ok = <-input
	}
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

func parseTimestamp(line string) (int64, error) {
	parse, err := time.Parse("20060102/150405.000", line)

	if err != nil {
		return -1, err
	}

	return parse.UnixMilli(), nil
}

func isNewRecord(line string) bool {
	return FILE_RECORD_RE.MatchString(line)
}
