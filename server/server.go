package server

import (
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/gorilla/mux"
	"github.com/simonz05/metrics/bitmap"
)

var (
	Version = "0.0.1"
	metrics *Metrics
	router  *mux.Router
)

type Metrics struct {
	bitmap *bitmap.Bitmap
}

func sigTrapCloser(l net.Listener) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for _ = range c {
			l.Close()
			Logf("Closed listener %s", l.Addr())
		}
	}()
}

func setupServer(redisUrl string) {
	conn, _ := bitmap.Open(redisUrl)
	metrics = &Metrics{
		bitmap.NewBitmap(conn),
	}

	// HTTP endpoints
	//http.HandleFunc("/api/1.0/track/", trackHandle)
	router = mux.NewRouter()
	router.HandleFunc("/api/1.0/track/", trackHandle).Methods("POST").Name("track")
	router.HandleFunc("/api/1.0/retention/", retentionHandle).Methods("GET").Name("retention")
	router.StrictSlash(false)
	http.Handle("/", router)
}

func ListenAndServe(laddr, redisUrl string) error {
	setupServer(redisUrl)
	l, err := net.Listen("tcp", laddr)

	if err != nil {
		return err
	}

	Logf("Listen on %s", l.Addr())

	sigTrapCloser(l)
	err = http.Serve(l, nil)
	Logf("Shutting down ..")
	return err
}
