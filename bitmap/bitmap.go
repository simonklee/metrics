package bitmap

import (
	"fmt"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Bitmap struct {
	conn Conn
}

func NewBitmap(c Conn) *Bitmap {
	return &Bitmap{c}
}

func (b *Bitmap) TrackAtTime(name string, id int, t time.Time) error {
	tt := timetuple(t)
	events := []Numeral{
		b.MonthEvent(name, tt[0], tt[1]),
		b.WeekEvent(name, tt[0], tt[4]),
		b.DayEvent(name, tt[0], tt[1], tt[2]),
		b.HourEvent(name, tt[0], tt[1], tt[2], tt[3]),
	}

	conn := b.conn.Get()
	defer conn.Close()

	conn.Send("MULTI")

	for _, ev := range events {
		conn.Send("SETBIT", ev.Key(), id, 1)
	}

	_, err := conn.Do("EXEC")
	return err
}

func (b *Bitmap) Track(name string, id int) error {
	return b.TrackAtTime(name, id, time.Now().UTC())
}

func (b *Bitmap) DeleteAllEvents() error {
	conn := b.conn.Get()
	defer conn.Close()

	res, err := redis.Values(conn.Do("KEYS", "tracklist:*"))

	if err != nil {
		return err
	}

	if len(res) > 0 {
		_, err := conn.Do("DEL", res...)
		return err
	}

	return nil
}

func (b *Bitmap) MonthEvent(name string, year, month int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d", name, year, month), b.conn}
}

func (b *Bitmap) MonthEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d", name, tt[0], tt[1]), b.conn}
}

func (b *Bitmap) WeekEvent(name string, year, week int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:W%d-%d", name, year, week), b.conn}
}

func (b *Bitmap) WeekEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:W%d-%d", name, tt[0], tt[4]), b.conn}
}

func (b *Bitmap) DayEvent(name string, year, month, day int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d", name, year, month, day), b.conn}
}

func (b *Bitmap) DayEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d", name, tt[0], tt[1], tt[2]), b.conn}
}

func (b *Bitmap) HourEvent(name string, year, month, day, hour int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d-%d", name, year, month, day, hour), b.conn}
}

func (b *Bitmap) HourEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d-%d", name, tt[0], tt[1], tt[2], tt[3]), b.conn}
}

type Numeral interface {
	Count() (int64, error)
	Contains(int) (bool, error)
	Delete() error
	Exists() (bool, error)
	Key() string
	Conn() Conn
}

type event struct {
	key  string
	conn Conn
}

func (ev *event) Delete() error {
	conn := ev.conn.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", ev.Key())
	return err
}

func (ev *event) Count() (int64, error) {
	conn := ev.conn.Get()
	defer conn.Close()

	return redis.Int64(conn.Do("BITCOUNT", ev.Key()))
}

func (ev *event) Contains(id int) (bool, error) {
	conn := ev.conn.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("GETBIT", ev.Key(), id))
}

func (ev *event) Exists() (bool, error) {
	conn := ev.conn.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("EXISTS", ev.Key()))
}

func (ev *event) Key() string {
	return ev.key
}

func (ev *event) Conn() Conn {
	return ev.conn
}

func (ev *event) String() string {
	return fmt.Sprintf("event(%s)", ev.key)
}

func bitOp(op string, numerals []Numeral) Numeral {
	n := len(numerals)

	if n < 1 {
		panic("bit op on less than one numeral")
	}

	keys := make([]string, n, n)
	ikeys := make([]interface{}, n+2, n+2)

	for i := 0; i < n; i++ {
		keys[i] = numerals[i].Key()
	}

	for i := 2; i < n+2; i++ {
		ikeys[i] = numerals[i-2].Key()
	}

	key := fmt.Sprintf("bitmap_bitop_%s_%s", op, strings.Join(keys, "-"))
	ev := &event{key, numerals[0].Conn()}
	ikeys[0] = op
	ikeys[1] = key

	conn := ev.conn.Get()
	defer conn.Close()

	conn.Do("BITOP", ikeys...)
	return ev
}

func AND(numerals ...Numeral) Numeral {
	return bitOp("AND", numerals)
}

func OR(numerals ...Numeral) Numeral {
	return bitOp("OR", numerals)
}

func XOR(numerals ...Numeral) Numeral {
	return bitOp("XOR", numerals)
}

func NOT(numerals ...Numeral) Numeral {
	return bitOp("NOT", numerals)
}

func timetuple(t time.Time) [5]int {
	year, month, day := t.Date()
	_, week := t.ISOWeek()
	return [5]int{year, int(month), day, t.Hour(), week}
}
