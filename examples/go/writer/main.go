package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	dariadb "github.com/lysevi/dariadb/go"
)

var paramsSuffix = [...]string{
	".raw",
	".average.minute",
	".median.minute",
	".average.halfhour",
	".median.halfhour",
	".average.hour", ".average.day"}

var hostname string
var readOnly bool
var paramsFrom int
var paramsTo int

var rawParams []string
var allParams []string
var db *dariadb.Dariadb

func init() {
	flag.StringVar(&hostname, "host", "http://localhost:2002", "host with dariadb")
	flag.IntVar(&paramsFrom, "from", 0, "first param number")
	flag.IntVar(&paramsTo, "to", 10, "last param number")
	flag.BoolVar(&readOnly, "readOnly", false, "readOnly")
	flag.Parse()
}

var values map[string]interface{}

type paramArray []string

func (a paramArray) Len() int           { return len(a) }
func (a paramArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a paramArray) Less(i, j int) bool { return a[i] < a[j] }

func printInfoValues() {
	curtime := time.Now()
	from := time.Date(curtime.Year(), curtime.Month(), curtime.Day(), 0, 0, 0, 0, time.UTC)
	to := time.Date(curtime.Year(), curtime.Month(), curtime.Day(), 23, 59, 59, 999999, time.UTC)

	res, err := db.Interval(allParams, dariadb.MakeTimestampFrom(from), dariadb.MakeTimestampFrom(to), dariadb.Flag(0))

	if err == nil {

		for _, k := range allParams {
			if v, ok := res[k]; ok {
				log.Printf("%v => %v\n", k, len(v))
			}
		}
	} else {
		log.Println(err)
	}
}

func initScheme() {
	localhostname, err := os.Hostname()
	if err != nil {
		localhostname = "localhost"
	}
	paramPrefix := localhostname + ".param"

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
	sort.Sort(paramArray(allParams))
}

func writeValues(ctx context.Context, paramname string) {
	var v = float64(0.0)

	for {
		select {
		case <-ctx.Done():
			break
		case <-time.After(50 * time.Millisecond):
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
	db = dariadb.New(hostname)

	initScheme()

	if !readOnly {

		ctx := context.Background()
		// var cancel context.CancelFunc
		//
		// ctx, cancel = context.WithCancel(context.Background())

		scheme, _ := db.Scheme()

		log.Println(scheme)
		for _, v := range rawParams {
			go writeValues(ctx, v)
		}
	}

	for {
		if readOnly {
			printInfoValues()
		}
		log.Println("***********************")
		time.Sleep(1000 * time.Millisecond)
	}
}
