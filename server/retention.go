package server

import (
	"log"
	"net/http"
	"strconv"
	"fmt"
	"time"
)

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func JsonError(w http.ResponseWriter, error string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{error: %s}`, error)
}

func retentionHandle(w http.ResponseWriter, r *http.Request) {
	unit := r.FormValue("unit")

	if !stringInSlice(unit, []string{"month", "week", "day", "hour"}) {
		JsonError(w, "Invalid unit", 400)
		return 
	}

	interval, err := strconv.Atoi(r.FormValue("interval"))

	if err != nil {
		JsonError(w, "Invalid interval", 400)
		return
	}

	const dateFmt = "2006-01-02"

	fromDate, err := time.Parse(dateFmt, r.FormValue("from_date"))

	if err != nil {
		JsonError(w, fmt.Sprintf("%s", err), 400)
		return
	}

	toDate, err := time.Parse(dateFmt, r.FormValue("to_date"))

	if err != nil {
		JsonError(w, "Invalid to_date", 400)
		return
	}

	log.Printf("unit: %s, interval: %d, time: %v-%v", unit, interval, fromDate, toDate)
}
