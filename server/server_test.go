package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"net/url"
	"io/ioutil"
	"log"
	"github.com/simonz05/metrics/bitmap"
)

var (
	once sync.Once
	serverAddr string
	server *httptest.Server
	redisUrl = "redis://:@localhost:6379/15"
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

	r, err := http.PostForm(fmt.Sprintf("http://%s/track/1/", serverAddr), values)

	if err != nil {
		log.Printf("error posting: %s", err)
		return
	}
	body, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()
	log.Printf("res: %s", body)
}
