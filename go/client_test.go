package dariadb

import (
	"math"
	"testing"
	"time"
)

func checkResult(allparams []string, values map[string][]Measurement, t *testing.T) {
	const TOLERANCE = 0.000001
	now := time.Now()
	for i, p := range allparams {
		if v, ok := values[p]; ok {
			testedTime := v[0].Timestamp.ToDate()
			if now.Hour() != testedTime.Hour() {
				t.Errorf("now.Hour() != v[0].Timestamp.ToDate().Hour(): %v %v", now, testedTime)
			}
			if len(v) < 1 {
				t.Errorf("len(v)<1")
			} else {
				m := v[0]
				if diff := math.Abs(float64(m.Value) - float64(i+1)); diff > TOLERANCE {
					t.Errorf("diff := math.Abs(v.Value-%v); diff > TOLERANCE: %v", i, m.Value)
				}
			}
		} else {
			t.Errorf("%v not found in readedInterval", p)
		}
	}
}

func TestAppend(t *testing.T) {
	client := New("http://localhost:2002")

	p, err := client.AddToScheme([]string{"param1.raw", "param2.raw"})
	if err != nil {
		t.Error(err)
	}

	if _, ok := p["param1.raw"]; !ok {
		t.Errorf("param1.raw not found in p")
	}

	if _, ok := p["param2.raw"]; !ok {
		t.Errorf("param2.raw not found in p")
	}

	ts := MakeTimestamp()
	err = client.Append("param1.raw", ts, Flag(0), Value(1))
	if err != nil {
		t.Error(err)
	}

	err = client.Append("param2.raw", ts, Flag(0), Value(2))
	if err != nil {
		t.Error(err)
	}

	scheme, err := client.Scheme()
	if err != nil {
		t.Error(err)
	}

	if _, ok := scheme["param1.raw"]; !ok {
		t.Errorf("param1.raw not found in scheme")
	}

	if _, ok := scheme["param2.raw"]; !ok {
		t.Errorf("param2.raw not found in scheme")
	}

	allparams := []string{"param1.raw", "param2.raw"}
	values, err := client.Interval(allparams, ts, ts+ts, Flag(0))
	if err != nil {
		t.Error(err)
	}
	checkResult(allparams, values, t)

	values, err = client.Timepoint(allparams, ts+ts, Flag(0))
	if err != nil {
		t.Error(err)
	}
	checkResult(allparams, values, t)

	fncs, err := client.StatFuncs()
	if err != nil {
		t.Error(err)
	}
	if len(fncs) < 2 {
		t.Errorf("len(fncs)<2: %v", fncs)
	}

	statresult, err := client.Statistic("param1.raw", fncs, ts, ts+ts, Flag(0))
	if err != nil {
		t.Error(err)
	}

	for _, f := range fncs {
		if _, ok := statresult[f]; !ok {
			t.Errorf("%v not found in statistic result", f)
		}
	}
}
