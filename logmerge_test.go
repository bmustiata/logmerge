package main

import (
	"strings"
	"testing"
)

func TestParseTimestamp(t *testing.T) {
	r, err := parseTimestamp("20220128/233741.111")

	if err != nil {
		t.Log("failure parsing timestamp", err)
		t.Fail()
	}

	if r != 1643413061111 {
		t.Logf("wrong value: %d", r)
		t.Fail()
	}
}

func TestIsNewRecord(t *testing.T) {
	content := "not prefixed by time thing"

	isEntry, timestamp := readNewEntry(content)
	if isEntry || timestamp > 0 {
		t.Logf(
			"The line:\n%s\nis not prefixed by time. It should indicate a continuation, not a new record.",
			content,
		)
		t.Fail()
	}

	content = "20220129/000102.900 - ;"

	isEntry, timestamp = readNewEntry(content)
	if !isEntry || timestamp < 0 {
		t.Logf(
			"The line:\n%s\nis prefixed by time. It should indicate a new record.",
			content,
		)
		t.Fail()
	}
}

func TestMultilineEntryReading(t *testing.T) {
	content := []string {
		"20220128/234741.000 - multiline the system",
		"  is starting",
		"  now",
	}

	// we keep the channel sized 100 to catch bugs when the multiline isn't
	// yielding multilines, but single lines. Otherwise, it would just lock,
	// since we read the first line in the for, the readMultilineLogEntry
	// writes one on its outChan, but since we're not yet reading, it will
	// die.
	lineChan := make(chan FileLine)
	outChan := readMultilineLogEntry(lineChan)

	// we write on a coroutine so we don't lock
	go func() {
		for _, line := range content {
			lineChan <- FileLine{
				fileName: "a.txt",
				line: line,
			}
		}

		close(lineChan)
	}()

	// FIXME: in real life this might leak coroutines, since the output chan isn't read
	//        anymore.
	outEntry := <- outChan

	expectedMergedContent := strings.Join(content, "\n")

	if expectedMergedContent != outEntry.content {
		t.Logf("Content differs. Expected:\n%s\nActual:\n%s\n",
			expectedMergedContent,
			outEntry.content)
		t.Fail()
	}
}