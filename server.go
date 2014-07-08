package main

import (
    "log"
    "net/http"
    "github.com/emicklei/go-restful"
    "github.com/gocql/gocql"
    "github.com/FlukeNetworks/timedb/timedb"
)

var tdb timedb.TimeDB

func main() {
    cluster := gocql.NewCluster("172.28.128.2")
    cluster.DefaultPort = 9042
    cluster.Keyspace = "timedb2"
    session, err := cluster.CreateSession()
    if err != nil {
        log.Fatal(err)
    }
    defer session.Close()
    tdb.Cacher = &timedb.CQLCacher{session}

    ws := new(restful.WebService)

    ws.Path("/v1/datapoints").Consumes(restful.MIME_JSON, restful.MIME_XML).Produces(restful.MIME_JSON, restful.MIME_XML)

    ws.Route(ws.POST("").To(insert).Reads(timedb.InputPoint{}))

    restful.Add(ws)
    http.ListenAndServe(":8080", nil)
}

func insert(req *restful.Request, res *restful.Response) {
    point := new(timedb.InputPoint)
    err := req.ReadEntity(&point)
    if err != nil {
        res.AddHeader("Content-Type", "text/plain")
        res.WriteErrorString(http.StatusInternalServerError, err.Error())
        return
    }
    err = tdb.Put(point)
    if err != nil {
        res.AddHeader("Content-Type", "text/plain")
        res.WriteErrorString(http.StatusInternalServerError, err.Error())
        return
    }
}
