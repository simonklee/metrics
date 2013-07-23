package bitmap

import (
	"fmt"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

var RedisPool = redis.Pool{
	MaxIdle:     128,
	IdleTimeout: 60 * time.Second,
	Dial: func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", ":6379")

		if err != nil {
			return nil, err
		}

		return c, err
	},
	TestOnBorrow: nil,
}

func TrackAtTime(name string, id int, t time.Time) error {
	tt := timetuple(t)
	events := []Numeral{
		MonthEvent(name, tt[0], tt[1]),
		WeekEvent(name, tt[0], tt[4]),
		DayEvent(name, tt[0], tt[1], tt[2]),
		HourEvent(name, tt[0], tt[1], tt[2], tt[3]),
	}

	conn := RedisPool.Get()
	defer conn.Close()

	conn.Send("MULTI")

	for _, ev := range events {
		conn.Send("SETBIT", ev.Key(), id, 1)
	}

	_, err := conn.Do("EXEC")
	return err
}

func Track(name string, id int) error {
	return TrackAtTime(name, id, time.Now().UTC())
}

func DeleteAllEvents() error {
	conn := RedisPool.Get()
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

type Numeral interface {
	Count() (int64, error)
	Contains(int) (bool, error)
	Delete() error
	Exists() (bool, error)
	Key() string
}

type event struct {
	key string
}

func (ev *event) Delete() error {
	conn := RedisPool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", ev.Key())
	return err
}

func (ev *event) Count() (int64, error) {
	conn := RedisPool.Get()
	defer conn.Close()

	return redis.Int64(conn.Do("BITCOUNT", ev.Key()))
}

func (ev *event) Contains(id int) (bool, error) {
	conn := RedisPool.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("GETBIT", ev.Key(), id))
}

func (ev *event) Exists() (bool, error) {
	conn := RedisPool.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("EXISTS", ev.Key()))
}

func (ev *event) Key() string {
	return ev.key
}

func (ev *event) String() string {
	return fmt.Sprintf("event(%s)", ev.key)
}

func BitOp(op string, numerals []Numeral) Numeral {
	n := len(numerals)
	keys := make([]string, n, n)
	ikeys := make([]interface{}, n+2, n+2)

	for i := 0; i < n; i++ {
		keys[i] = numerals[i].Key()
	}

	for i := 2; i < n+2; i++ {
		ikeys[i] = numerals[i-2].Key()
	}

	key := fmt.Sprintf("trackist_bitop_%s_%s", op, strings.Join(keys, "-"))
	ev := &event{key}
	ikeys[0] = op
	ikeys[1] = key

	conn := RedisPool.Get()
	defer conn.Close()

	conn.Do("BITOP", ikeys...)
	return ev
}

func AND(numerals ...Numeral) Numeral {
	return BitOp("AND", numerals)
}

func OR(numerals ...Numeral) Numeral {
	return BitOp("OR", numerals)
}

func XOR(numerals ...Numeral) Numeral {
	return BitOp("XOR", numerals)
}

func NOT(numerals ...Numeral) Numeral {
	return BitOp("NOT", numerals)
}

func MonthEvent(name string, year, month int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d", name, year, month)}
}

func MonthEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d", name, tt[0], tt[1])}
}

func WeekEvent(name string, year, week int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:W%d-%d", name, year, week)}
}

func WeekEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:W%d-%d", name, tt[0], tt[4])}
}

func DayEvent(name string, year, month, day int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d", name, year, month, day)}
}

func DayEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d", name, tt[0], tt[1], tt[2])}
}

func HourEvent(name string, year, month, day, hour int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d-%d", name, year, month, day, hour)}
}

func HourEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d-%d", name, tt[0], tt[1], tt[2], tt[3])}
}

func timetuple(t time.Time) [5]int {
	year, month, day := t.Date()
	_, week := t.ISOWeek()
	return [5]int{year, int(month), day, t.Hour(), week}
}
