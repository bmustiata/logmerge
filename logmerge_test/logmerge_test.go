package logmerge_test

import (
	"testing"
	"github.com/bmustiata/logmerge"
)

func TestParseTimestamp(t *testing.T) {
	r, err := logmerge.ParseTimestamp("20220128/233741.111")

	if err != nil {
		t.Log("Failure parsing timestamp", err)
		t.Fail()
	}

	if r != 123 {
		t.Logf("Wrong value: %d", r)
		t.Fail()
	}
}
