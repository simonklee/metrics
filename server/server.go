package server

import (
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/gorilla/mux"
)

var Version = "0.0.1"

var router *mux.Router

func indexHandle(w http.ResponseWriter, r *http.Request) {
}

func trackHandle(w http.ResponseWriter, r *http.Request) {
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

func ListenAndServe(laddr string) error {
	// HTTP endpoints
	router = mux.NewRouter()
	router.HandleFunc("/", indexHandle).Name("index")
	router.HandleFunc("/track/{id}/", trackHandle).Methods("POST").Name("track")
	router.StrictSlash(false)
	http.Handle("/", router)

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


