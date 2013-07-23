package bitmap

import (
	"testing"
	"time"
)

type Time struct {
	Year  int
	Month int
	Day   int
	Hour  int
	Week  int
}

func newTime(now time.Time) *Time {
	year, _month, day := now.Date()
	month := int(_month)
	_, week := now.ISOWeek()
	hour := now.Hour()
	return &Time{year, month, day, hour, week}
}

type DayTest struct {
	ev *Event
	id int
	ok bool
}

func TestWithDifferentDays(t *testing.T) {
	if err := DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	if err := Track("active", 123); err != nil {
		t.Fatalf("track failed, got %v\n", err)
	}

	n := newTime(time.Now().UTC())

	dayTests := []DayTest{
		{MonthEvent("active", n.Year, n.Month), 123, true},
		{MonthEvent("active", n.Year, n.Month-1), 123, false},
		{WeekEvent("active", n.Year, n.Week), 123, true},
		{WeekEvent("active", n.Year, n.Week-1), 123, false},
		{DayEvent("active", n.Year, n.Month, n.Day), 123, true},
		{DayEvent("active", n.Year, n.Month, n.Day-2), 123, false},
		{HourEvent("active", n.Year, n.Month, n.Day, n.Hour), 123, true},
		{HourEvent("active", n.Year, n.Month, n.Day, n.Hour-1), 123, false},
		{HourEvent("active", n.Year, n.Month, n.Day, n.Hour), 124, false},
	}

	for _, test := range dayTests {
		if ok, err := test.ev.Contains(test.id); ok != test.ok || err != nil {
			t.Fatalf("%s Contains(`%d`) expected %v, err(%v)", test.ev, test.id, test.ok, err)
		}
	}
}

func TestCounts(t *testing.T) {
	if err := DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()

	if cnt, err := MonthEventAtTime("active", now).Count(); cnt != 0 || err != nil {
		t.Fatalf("Expected count 0, got %d. Error(%v)", cnt, err)
	}

	Track("active", 123)
	Track("active", 23232)

	if cnt, err := MonthEventAtTime("active", now).Count(); cnt != 2 || err != nil {
		t.Fatalf("Expected count 2, got %d. Error(%v)", cnt, err)
	}
}

func TestDifferentDates(t *testing.T) {
	if err := DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, now.Day()-1)

	if err := TrackAtTime("active", 123, now); err != nil {
		t.Fatalf("track failed, got %v\n", err)
	}

	if err := TrackAtTime("active", 123, yesterday); err != nil {
		t.Fatalf("track failed, got %v\n", err)
	}

	if cnt, err := DayEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if cnt, err := DayEventAtTime("active", yesterday).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}
}

func TestDifferentBuckets(t *testing.T) {
	if err := DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()

	Track("active", 123)
	Track("tasks:completed", 23232)

	if cnt, err := MonthEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if cnt, err := MonthEventAtTime("tasks:completed", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}
}

func TestExists(t *testing.T) {
	if err := DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()

	if cnt, err := MonthEventAtTime("active", now).Count(); cnt != 0 || err != nil {
		t.Fatalf("Expected count 0, got %d. Error(%v)", cnt, err)
	}

	if ok, err := MonthEventAtTime("active", now).Exists(); ok != false || err != nil {
		t.Fatalf("Expected false, got %v. Error(%v)", ok, err)
	}

	TrackAtTime("active", 123, now)

	if cnt, err := MonthEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if ok, err := MonthEventAtTime("active", now).Exists(); ok != true || err != nil {
		t.Fatalf("Expected count 1, got %v. Error(%v)", ok, err)
	}
}

type OpTest struct {
	name     string
	op       string
	numerals []Numeral
	count    int
	ids      []int
	notIds   []int
}

func TestOp(t *testing.T) {
	if err := DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()
	monthAgo := now.AddDate(0, int(now.Month())-1, 0)

	// 123 has been active for two months
	TrackAtTime("active", 123, now)
	TrackAtTime("active", 123, monthAgo)

	// 124 has only been active last month
	TrackAtTime("active", 124, monthAgo)

	if cnt, err := MonthEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if cnt, err := MonthEventAtTime("active", monthAgo).Count(); cnt != 2 || err != nil {
		t.Fatalf("Expected count 2, got %d. Error(%v)", cnt, err)
	}

	opTests := []OpTest{
		{
			name: "bitop AND #1",
			op:   "AND",
			numerals: []Numeral{
				MonthEventAtTime("active", monthAgo),
				MonthEventAtTime("active", now),
			},
			count:  1,
			ids:    []int{123},
			notIds: []int{124},
		},
		{
			name: "bitop AND #2",
			op:   "AND",
			numerals: []Numeral{
				MonthEventAtTime("active", monthAgo),
			},
			count: 2,
			ids:   []int{123, 124},
		},
		{
			name: "bitop nested AND #3",
			op:   "AND",
			numerals: []Numeral{
				AND(MonthEventAtTime("active", monthAgo),
					MonthEventAtTime("active", now)),
				MonthEventAtTime("active", now),
			},
			count:  1,
			ids:    []int{123},
			notIds: []int{124},
		},
		{
			name: "bitop OR #1",
			op:   "OR",
			numerals: []Numeral{
				MonthEventAtTime("active", monthAgo),
				MonthEventAtTime("active", now),
			},
			count:  2,
			ids:    []int{123, 124},
			notIds: nil,
		},
		{
			name: "bitop XOR #1",
			op:   "XOR",
			numerals: []Numeral{
				MonthEventAtTime("active", monthAgo),
				MonthEventAtTime("active", now),
			},
			count:  1,
			ids:    []int{124},
			notIds: []int{123},
		},
	}

	for _, test := range opTests {
		rv := BitOp(test.op, test.numerals)

		if cnt, err := rv.Count(); int(cnt) != test.count || err != nil {
			t.Fatalf("%s: Expected count %d, got %d. Error(%v)", test.name, test.count, cnt, err)
		}

		for _, id := range test.ids {
			if ok, err := rv.Contains(id); ok != true || err != nil {
				t.Fatalf("%s: Expected id %d not available. Error(%v)", test.name, id, err)
			}
		}

		for _, id := range test.notIds {
			if ok, err := rv.Contains(id); ok != false || err != nil {
				t.Fatalf("%s: Not Expected id %d was available. Error(%v)", test.name, id, err)
			}
		}
	}
}
