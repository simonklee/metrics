package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
)

type Track struct {
	Name string `json:"name"`
	Id   int    `json:"id"`
}

func (e *Track) String() string {
	return fmt.Sprintf(`
Name: %s
Id: %s`, e.Name, e.Id)
}

func trackHandle(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	id, err := strconv.Atoi(r.FormValue("id"))

	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	log.Println("name", name)
	log.Println("id", id)

	if err := metrics.bitmap.Track(name, id); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.WriteHeader(201)
}
