package main

import (
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
	if isNewRecord(content) {
		t.Logf(
			"The line:\n%s\nis not prefixed by time. It should indicate a continuation, not a new record.",
			content,
		)
		t.Fail()
	}

	content = "20220129/000102.900 - ;"
	if ! isNewRecord(content) {
		t.Logf(
			"The line:\n%s\nis prefixed by time. It should indicate a new record.",
			content,
		)
		t.Fail()
	}
}
