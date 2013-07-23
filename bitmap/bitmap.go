package bitmap

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Client struct {
	pool           redis.Pool
	addr, password string
	db             uint8
}

func NewClient(rawurl string) *Client {
	c := new(Client)

	if rawurl == "" {
		rawurl = "redis://:@localhost:6379/0"
	}

	u, err := url.Parse(rawurl)

	if err != nil {
		log.Fatal(err)
	}

	if pass, ok := u.User.Password(); ok {
		c.password = pass
	}

	db := u.Path

	if len(db) > 1 && db[0] == '\\' {
		db = db[1:len(db)]
	}

	n, err := strconv.ParseUint(db, 10, 8)

	if err != nil {
		n = 0
	}

	c.db = uint8(n)
	c.addr = u.Host

	c.pool = redis.Pool{
		MaxIdle:     128,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			return c.dial()
		},
		TestOnBorrow: nil,
	}
	return c
}

func (c *Client) dial() (redis.Conn, error) {
	conn, err := redis.Dial("tcp", c.addr)

	if err != nil {
		return nil, err
	}

	//if LogLevel >= 2 {
	//	conn = redis.NewLoggingConn(conn, Logger, "")
	//}

	if c.password != "" {
		if _, err := conn.Do("AUTH", c.password); err != nil {
			//Logln("h: invalid redis password")
			conn.Close()
			return nil, err
		}
	}

	if c.db != 0 {
		if _, err := conn.Do("SELECT", c.db); err != nil {
			//Logln("h: invalid redis password")
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}

func (c *Client) TrackAtTime(name string, id int, t time.Time) error {
	tt := timetuple(t)
	events := []Numeral{
		c.MonthEvent(name, tt[0], tt[1]),
		c.WeekEvent(name, tt[0], tt[4]),
		c.DayEvent(name, tt[0], tt[1], tt[2]),
		c.HourEvent(name, tt[0], tt[1], tt[2], tt[3]),
	}

	conn := c.pool.Get()
	defer conn.Close()

	conn.Send("MULTI")

	for _, ev := range events {
		conn.Send("SETBIT", ev.Key(), id, 1)
	}

	_, err := conn.Do("EXEC")
	return err
}

func (c *Client) Track(name string, id int) error {
	return c.TrackAtTime(name, id, time.Now().UTC())
}

func (c *Client) DeleteAllEvents() error {
	conn := c.pool.Get()
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

func (c *Client) MonthEvent(name string, year, month int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d", name, year, month), c}
}

func (c *Client) MonthEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d", name, tt[0], tt[1]), c}
}

func (c *Client) WeekEvent(name string, year, week int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:W%d-%d", name, year, week), c}
}

func (c *Client) WeekEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:W%d-%d", name, tt[0], tt[4]), c}
}

func (c *Client) DayEvent(name string, year, month, day int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d", name, year, month, day), c}
}

func (c *Client) DayEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d", name, tt[0], tt[1], tt[2]), c}
}

func (c *Client) HourEvent(name string, year, month, day, hour int) Numeral {
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d-%d", name, year, month, day, hour), c}
}

func (c *Client) HourEventAtTime(name string, t time.Time) Numeral {
	tt := timetuple(t)
	return &event{fmt.Sprintf("tracklist:%s:%d-%d-%d-%d", name, tt[0], tt[1], tt[2], tt[3]), c}
}

type Numeral interface {
	Count() (int64, error)
	Contains(int) (bool, error)
	Delete() error
	Exists() (bool, error)
	Key() string
	Client() *Client
}

type event struct {
	key    string
	client *Client
}

func (ev *event) Delete() error {
	conn := ev.client.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", ev.Key())
	return err
}

func (ev *event) Count() (int64, error) {
	conn := ev.client.pool.Get()
	defer conn.Close()

	return redis.Int64(conn.Do("BITCOUNT", ev.Key()))
}

func (ev *event) Contains(id int) (bool, error) {
	conn := ev.client.pool.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("GETBIT", ev.Key(), id))
}

func (ev *event) Exists() (bool, error) {
	conn := ev.client.pool.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("EXISTS", ev.Key()))
}

func (ev *event) Key() string {
	return ev.key
}

func (ev *event) Client() *Client {
	return ev.client
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
	ev := &event{key, numerals[0].Client()}
	ikeys[0] = op
	ikeys[1] = key

	conn := ev.client.pool.Get()
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
