package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gizak/termui"
	dariadb "github.com/lysevi/dariadb/go"
)

var server string
var params string
var db *dariadb.Dariadb
var bclabels []string
var barcharts []*termui.BarChart
var labelsValues [][]int
var par2num map[string]int // param name to number in labels and charts

func init() {
	flag.StringVar(&server, "host", "http://localhost:2002", "host with dariadb")
	flag.StringVar(&params, "params", "param1;param2;param3", "';' separated list of params")
	flag.Parse()
}

var values map[string]interface{}

type paramArray []string

func (a paramArray) Len() int           { return len(a) }
func (a paramArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a paramArray) Less(i, j int) bool { return a[i] < a[j] }

func printInfoValues(ctx context.Context, params []string) {
	fmt.Println("printInfoValues started", params)
	for {
		select {
		case <-ctx.Done():
			break
		case <-time.After(500 * time.Millisecond):
			{
				curtime := time.Now()
				from := time.Date(curtime.Year(), curtime.Month(), curtime.Day(), 0, 0, 0, 0, time.UTC)
				to := time.Date(curtime.Year(), curtime.Month(), curtime.Day(), 23, 59, 59, 999999, time.UTC)

				res, err := db.Interval(params, dariadb.MakeTimestampFrom(from), dariadb.MakeTimestampFrom(to), dariadb.Flag(0))

				if err == nil {
					for _, k := range params {
						if v, ok := res[k]; ok {
							num := par2num[k]
							targetData := &labelsValues[num]

							for i := 0; i < 24; i++ {
								(*targetData)[i] = 0
							}
							for _, meas := range v {
								dt := meas.Timestamp.ToDate()
								(*targetData)[dt.Hour()] = (*targetData)[dt.Hour()] + 1
							}
						}

					}
				} else {
					panic(err)
				}
			}
		}

	}
}

func main() {
	var paramlist = strings.Split(params, ";")

	for i := 0; i < 24; i++ {
		bclabels = append(bclabels, strconv.Itoa(i))
	}
	fmt.Println(bclabels)
	db = dariadb.New(server)

	ctx := context.Background()
	var cancel context.CancelFunc

	ctx, cancel = context.WithCancel(context.Background())

	go printInfoValues(ctx, paramlist)

	if err := termui.Init(); err != nil {
		panic(err)
	}
	defer termui.Close()
	par2num = make(map[string]int)
	for i, p := range paramlist {
		par2num[p] = i
		data := make([]int, 24)
		labelsValues = append(labelsValues, data)
		bc := termui.NewBarChart()

		bc.BorderLabel = p
		bc.Data = data
		bc.Width = 40
		bc.Height = 15
		bc.DataLabels = bclabels
		bc.TextColor = termui.ColorGreen
		bc.BarColor = termui.ColorRed
		bc.NumColor = termui.ColorYellow

		barcharts = append(barcharts, bc)

		termui.Body.AddRows(
			termui.NewRow(
				termui.NewCol(9, 0, bc)))
	}

	termui.Body.Align()

	termui.Render(termui.Body)

	termui.Handle("/sys/kbd/q", func(termui.Event) {
		termui.StopLoop()
		cancel()
	})
	draw := func() {
		for i := 0; i < len(barcharts); i++ {
			barcharts[i].Data = labelsValues[i]
			termui.Render(barcharts[i])
		}
	}
	termui.Handle("/timer/1s", func(termui.Event) {
		draw()
	})
	termui.Loop()
}
