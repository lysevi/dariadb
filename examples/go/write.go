package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	dariadb "github.com/lysevi/dariadb/go"
)

type query struct {
	queryJSON []byte
	paramname string
	v         float64
}

var asyncWriterChanel chan query
var networkMutex sync.Mutex

var server string
var disableReader bool
var paramsFrom int
var paramsTo int

var rawParams []string
var allParams []string
var db *dariadb.Dariadb

func init() {
	flag.StringVar(&server, "host", "http://localhost:2002", "host with dariadb")
	flag.IntVar(&paramsFrom, "from", 0, "first param number")
	flag.IntVar(&paramsTo, "to", 10, "last param number")
	flag.BoolVar(&disableReader, "disableReader", false, "enable reader")
	flag.Parse()
}

var values map[string]interface{}

func printInfoValues() {
	curtime := time.Now()
	from := time.Date(curtime.Year(), curtime.Month(), curtime.Day(), 0, 0, 0, 0, time.UTC)
	to := time.Date(curtime.Year(), curtime.Month(), curtime.Day(), 23, 59, 59, 999999, time.UTC)

	res, err := db.Interval(allParams, dariadb.MakeTimestampFrom(from), dariadb.MakeTimestampFrom(to), dariadb.Flag(0))

	if err == nil {
		for k, v := range res {
			log.Printf("%v => %v", k, len(v))
		}
	}
}

func initScheme() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	paramPrefix := hostname + ".param"
	paramsSuffix := [...]string{".raw", ".average.minute", ".median.minute", ".average.halfhour", ".median.halfhour", ".average.hour", ".average.day"}

	paramsCount := paramsTo - paramsFrom
	rawParams = make([]string, 0, paramsCount)
	allParams = make([]string, 0, paramsCount*len(paramsSuffix))

	for i := paramsFrom; i < paramsTo; i++ {
		params := make([]string, len(paramsSuffix))
		j := 0
		for _, suff := range paramsSuffix {
			params[j] = fmt.Sprintf("%s_%d%s", paramPrefix, i, suff)
			allParams = append(allParams, params[j])
			j++
		}
		rawParams = append(rawParams, params[0])
		db.AddToScheme(params)
	}
}

func writeValues(ctx context.Context, paramname string) {
	var v = float64(0.0)

	for {
		select {
		case <-ctx.Done():
			break
		case <-time.After(100 * time.Millisecond):
			err := db.Append(paramname, dariadb.MakeTimestamp(), dariadb.Flag(0), dariadb.Value(v))
			if err != nil {
				log.Printf("append error: %v", err)
			} else {
				log.Printf("%v = %v", paramname, v)
			}
			v = (v + 0.1)
			if v > 1000 {
				v = 0.0
			}

		}
	}
}

func main() {
	db = dariadb.New(server)

	initScheme()

	ctx := context.Background()
	asyncWriterChanel = make(chan query)
	// var cancel context.CancelFunc
	//
	// ctx, cancel = context.WithCancel(context.Background())

	scheme, _ := db.Scheme()

	log.Println(scheme)
	for _, v := range rawParams {
		go writeValues(ctx, v)
	}

	for {
		if !disableReader {
			printInfoValues()
		}
		time.Sleep(time.Second)
	}
}
