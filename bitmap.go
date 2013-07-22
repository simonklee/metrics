package bitmap

import (
	"time"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

var pool = redis.Pool{
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


func Track(name string, id int) error {
	now := time.Now().UTC()
	year, month, day := now.Date()
	hour := now.Hour()
	isoyear, week := now.ISOWeek()

	events := []*Event{
		MonthEvent(name, year, int(month)),
		WeekEvent(name, isoyear, week),
		DayEvent(name, year, week, day),
		HourEvent(name, year, week, day, hour),
	}

	conn := pool.Get()
	defer conn.Close()

	conn.Send("MULTI")

	for _, ev := range(events) {
		conn.Send("SETBIT", ev.Key(), id, 1)
	}

	_, err := conn.Do("EXEC")
	return err
}

func DeleteAllEvents() error {
	conn := pool.Get()
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

type Op interface {
	Count() (int64, error)
	Contains(int64) bool
	Key() string
}

type Event struct {
	key string
}

func (ev* Event) Count() (int64, error) {
	conn := pool.Get()
	defer conn.Close()
	
	return redis.Int64(conn.Do("BITCOUNT", ev.Key()))
}

func (ev* Event) Key() string {
	return ev.key
}

func MonthEvent(name string, year, month int) *Event {
	return &Event{fmt.Sprintf("tracklist:%s:%d-%d", name, year, month)}
}

func WeekEvent(name string, year, week int) *Event {
	return &Event{fmt.Sprintf("tracklist:%s:W%d-%d", name, year, week)}
}

func DayEvent(name string, year, month, day int) *Event {
	return &Event{fmt.Sprintf("tracklist:%s:%d-%d-%d", name, year, month, day)}
}

func HourEvent(name string, year, month, day, hour int) *Event {
	return &Event{fmt.Sprintf("tracklist:%s:%d-%d-%d-%d", name, year, month, day, hour)}
}
