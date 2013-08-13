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
	ev Numeral
	id int
	ok bool
}

func getBitmap() *Bitmap {
	c, _ := Open("redis://:@localhost:6379/15")
	return NewBitmap(c)
}

func TestServerConf(t *testing.T) {
	b := getBitmap()
	b.DeleteAllEvents()
}

func TestWithDifferentDays(t *testing.T) {
	b := getBitmap()

	if err := b.DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	if err := b.Track("active", 123); err != nil {
		t.Fatalf("track failed, got %v\n", err)
	}

	n := newTime(time.Now().UTC())

	dayTests := []DayTest{
		{b.MonthEvent("active", n.Year, n.Month), 123, true},
		{b.MonthEvent("active", n.Year, n.Month-1), 123, false},
		{b.WeekEvent("active", n.Year, n.Week), 123, true},
		{b.WeekEvent("active", n.Year, n.Week-1), 123, false},
		{b.DayEvent("active", n.Year, n.Month, n.Day), 123, true},
		{b.DayEvent("active", n.Year, n.Month, n.Day-2), 123, false},
		{b.HourEvent("active", n.Year, n.Month, n.Day, n.Hour), 123, true},
		{b.HourEvent("active", n.Year, n.Month, n.Day, n.Hour-1), 123, false},
		{b.HourEvent("active", n.Year, n.Month, n.Day, n.Hour), 124, false},
	}

	for _, test := range dayTests {
		if ok, err := test.ev.Contains(test.id); ok != test.ok || err != nil {
			t.Fatalf("%s Contains(`%d`) expected %v, err(%v)", test.ev, test.id, test.ok, err)
		}
	}
}

func TestCounts(t *testing.T) {
	b := getBitmap()
	if err := b.DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()

	if cnt, err := b.MonthEventAtTime("active", now).Count(); cnt != 0 || err != nil {
		t.Fatalf("Expected count 0, got %d. Error(%v)", cnt, err)
	}

	b.Track("active", 123)
	b.Track("active", 23232)

	if cnt, err := b.MonthEventAtTime("active", now).Count(); cnt != 2 || err != nil {
		t.Fatalf("Expected count 2, got %d. Error(%v)", cnt, err)
	}
}

func TestDifferentDates(t *testing.T) {
	b := getBitmap()

	if err := b.DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, now.Day()-1)

	if err := b.TrackAtTime("active", 123, now); err != nil {
		t.Fatalf("track failed, got %v\n", err)
	}

	if err := b.TrackAtTime("active", 123, yesterday); err != nil {
		t.Fatalf("track failed, got %v\n", err)
	}

	if cnt, err := b.DayEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if cnt, err := b.DayEventAtTime("active", yesterday).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}
}

func TestDifferentBuckets(t *testing.T) {
	b := getBitmap()
	if err := b.DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()

	b.Track("active", 123)
	b.Track("tasks:completed", 23232)

	if cnt, err := b.MonthEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if cnt, err := b.MonthEventAtTime("tasks:completed", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}
}

func TestExists(t *testing.T) {
	b := getBitmap()

	if err := b.DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()

	if cnt, err := b.MonthEventAtTime("active", now).Count(); cnt != 0 || err != nil {
		t.Fatalf("Expected count 0, got %d. Error(%v)", cnt, err)
	}

	if ok, err := b.MonthEventAtTime("active", now).Exists(); ok != false || err != nil {
		t.Fatalf("Expected false, got %v. Error(%v)", ok, err)
	}

	b.TrackAtTime("active", 123, now)

	if cnt, err := b.MonthEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if ok, err := b.MonthEventAtTime("active", now).Exists(); ok != true || err != nil {
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
	b := getBitmap()
	if err := b.DeleteAllEvents(); err != nil {
		t.Fatalf("DeleteAllEvents failed, got %v\n", err)
	}

	now := time.Now().UTC()
	monthAgo := now.AddDate(0, int(now.Month())-1, 0)

	// 123 has been active for two months
	b.TrackAtTime("active", 123, now)
	b.TrackAtTime("active", 123, monthAgo)

	// 124 has only been active last month
	b.TrackAtTime("active", 124, monthAgo)

	if cnt, err := b.MonthEventAtTime("active", now).Count(); cnt != 1 || err != nil {
		t.Fatalf("Expected count 1, got %d. Error(%v)", cnt, err)
	}

	if cnt, err := b.MonthEventAtTime("active", monthAgo).Count(); cnt != 2 || err != nil {
		t.Fatalf("Expected count 2, got %d. Error(%v)", cnt, err)
	}

	opTests := []OpTest{
		{
			name: "bitop AND #1",
			op:   "AND",
			numerals: []Numeral{
				b.MonthEventAtTime("active", monthAgo),
				b.MonthEventAtTime("active", now),
			},
			count:  1,
			ids:    []int{123},
			notIds: []int{124},
		},
		{
			name: "bitop AND #2",
			op:   "AND",
			numerals: []Numeral{
				b.MonthEventAtTime("active", monthAgo),
			},
			count: 2,
			ids:   []int{123, 124},
		},
		{
			name: "bitop nested AND #3",
			op:   "AND",
			numerals: []Numeral{
				AND(b.MonthEventAtTime("active", monthAgo),
					b.MonthEventAtTime("active", now)),
				b.MonthEventAtTime("active", now),
			},
			count:  1,
			ids:    []int{123},
			notIds: []int{124},
		},
		{
			name: "bitop OR #1",
			op:   "OR",
			numerals: []Numeral{
				b.MonthEventAtTime("active", monthAgo),
				b.MonthEventAtTime("active", now),
			},
			count:  2,
			ids:    []int{123, 124},
			notIds: nil,
		},
		{
			name: "bitop XOR #1",
			op:   "XOR",
			numerals: []Numeral{
				b.MonthEventAtTime("active", monthAgo),
				b.MonthEventAtTime("active", now),
			},
			count:  1,
			ids:    []int{124},
			notIds: []int{123},
		},
	}

	for _, test := range opTests {
		rv := bitOp(test.op, test.numerals)

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
