package bitmap

import (
	"testing"
)

func TestWithDifferentDays(t* testing.T) {
	if err := DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}
}
