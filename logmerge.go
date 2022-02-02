package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
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

func readLogRecord(input chan string, output chan FileRecord) {
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

func filterLogRecord(input chan FileRecord) (chan FileRecord, error) {
	record, ok := <-input
	output := make(chan FileRecord)

	// FIXME: when the record exit the window bounds, we should just close the input stream
	go func() {
		for ok {
			if isRecordValid(record) {
				output <- record
			}

			record, ok = <-input
		}

		close(output)
	}()


	return output, nil
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

	defer f.Close()
	r := bufio.NewWriter(f)

	record, ok := <-input

	for ok {
		_, err = r.WriteString(record.content)

		if err != nil {
			log.Fatal(fmt.Errorf("unable to write into output file %s: %w", outFileName, err))
		}

		record, ok = <-input
	}
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

func main() {
	files := []string { "f1", "f2" }
	lineChannels, _ := createLineChannels(files)
	multilineLogEntryChannels, _ := createContentChannels(lineChannels)
	recordChannels, _ := createRecordChannels(multilineLogEntryChannels)
	orderedRecordChannel, _ := createOrderByTimeChannel(recordChannels)
	fiteredResultsChannel, _ := filterLogRecord(orderedRecordChannel)
	writeLog("/tmp/txt", fiteredResultsChannel)
}

// createOrderByTimeChannel Reads all the channels and returns the next row in order
func createOrderByTimeChannel(channels []chan FileRecord) (chan FileRecord, error) {
	result := make(chan FileRecord)

	// FIXME: implement sorting

	return result, nil
}

func createLineChannels(files []string) ([]chan string, error) {
	result := make([]chan string, len(files))

	for i, file := range files {
		c := make(chan string)
		go readFileLines(file, c)

		result[i] = c
	}

	return result, nil
}

func createContentChannels(lineChannels []chan string) ([]chan string, error) {
	result := make([]chan string, len(lineChannels))

	for i, lineChannel := range lineChannels {
		c := make(chan string)
		go readMultilineLogEntry(lineChannel, c)

		result[i] = c
	}

	return result, nil
}

func createRecordChannels(contentChannels []chan string) ([]chan FileRecord, error) {
	result := make([]chan FileRecord, len(contentChannels))

	for i, file := range contentChannels {
		c := make(chan FileRecord)
		go readLogRecord(file, c)

		result[i] = c
	}

	return result, nil
}
