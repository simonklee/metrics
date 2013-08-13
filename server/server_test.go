package server

import (
	"fmt"
	"github.com/simonz05/metrics/bitmap"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
)

var (
	once       sync.Once
	serverAddr string
	server     *httptest.Server
	redisUrl   = "redis://:@localhost:6379/15"
)

func startServer() {
	LogLevel = 1
	setupServer(redisUrl)
	server = httptest.NewServer(nil)
	serverAddr = server.Listener.Addr().String()
}

func emptyDb() {
	bitmap.NewClient(redisUrl).DeleteAllEvents()
}

func TestTrack(t *testing.T) {
	once.Do(startServer)
	values := make(url.Values)
	values.Set("name", "active")
	values.Set("id", "123")

	r, err := http.PostForm(fmt.Sprintf("http://%s/api/1.0/track/", serverAddr), values)

	if err != nil {
		log.Printf("error posting: %s", err)
		return
	}

	if r.StatusCode != 201 {
		t.Fatalf("expected status code 201, got %d", r.StatusCode)
	}

	body, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()
	log.Printf("res: %s", body)
}

func TestRetention(t *testing.T) {
	once.Do(startServer)
	values := url.Values{
		"unit": {"day"},
		"interval": {"12"},
		"from_date": {"2013-06-01"},
		"to_date": {"2013-06-12"},
	}

	r, err := http.Get(fmt.Sprintf("http://%s/api/1.0/retention/?%s", serverAddr, values.Encode()))

	if err != nil {
		log.Printf("error posting: %s", err)
		return
	}

	body, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()
	log.Printf("res: %s", body)

	if r.StatusCode != 200 {
		t.Fatalf("expected status code 200, got %d", r.StatusCode)
	}
}
