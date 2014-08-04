package main

import (
	"flag"
	"fmt"
	"github.com/FlukeNetworks/timedb"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"net/http"
)

const (
	DefaultPort = 8080
)

func main() {
	// Setup flags
	port := flag.Int("port", DefaultPort, "port on which to listen")
	flag.Parse()

	// Setup routes
	r := mux.NewRouter()
	r.HandleFunc("/series", ListSeries).Methods("GET")
	r.HandleFunc("/series/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", InsertPoint).Methods("POST")

	// Setup basic recovery and logging middleware
	n := negroni.Classic()
	n.UseHandler(r)
	n.Run(fmt.Sprintf(":%d", *port))
}

func ListSeries(res http.ResponseWriter, req *http.Request) {
	res.WriteHeader(http.StatusNotImplemented)
}

func InsertPoint(res http.ResponseWriter, req *http.Request) {
	res.WriteHeader(http.StatusNotImplemented)
}
