package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

var router *mux.Router

// setup request multiplexer
func regMux() *mux.Router {

	// HTTP endpoints
	router = mux.NewRouter()
	router.HandleFunc("/", indexHandle).Name("index")
	router.HandleFunc("/track/{id}/", trackHandle).Methods("POST").Name("track")
	router.StrictSlash(false)
	http.Handle("/", router)

	return router
}

type Track struct {
	Name string `json:"name"`
	Id   int    `json:"id"`
}

func (e *Track) String() string {
	return fmt.Sprintf(`
Name: %s
Id: %s`, e.Name, e.Id)
}

func indexHandle(w http.ResponseWriter, r *http.Request) {
}

func trackHandle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	appId := vars["id"]
	name := r.FormValue("name")
	id, err := strconv.Atoi(r.FormValue("id"))

	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	log.Println("appid", appId)
	log.Println("name", name)
	log.Println("id", id)
	metrics.bitmap.Track(name, id)
}
