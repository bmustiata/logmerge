package logmerge

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

type FileRecord struct {
	timestamp int64
	content   string
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
		ts, _ := ParseTimestamp(line)
		output <- FileRecord{
			content:   line,
			timestamp: ts,
		}

		line, ok = <-input
	}
}

func filterLogRecord(input, output chan FileRecord) {
	defer close(output)
	record, ok := <-input

	// FIXME: when the record exit the window bounds, we should just close the input stream
	for ok {
		if isRecordValid(record) {
			output <- record
		}
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

func ParseTimestamp(line string) (int64, error) {
	return 0, nil
}

func isNewRecord(line string) bool {
	return true
}

func main() {

	fmt.Println("vim-go")
}
