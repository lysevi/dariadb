package dariadb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"
)

// ID - meas
type ID int64

// Time - timestamp in milliseconds
type Time int64

// Flag - measurement flag
type Flag int32

// Value of measurement
type Value float64

// Measurement - value from dariadb
type Measurement struct {
	Timestamp Time  `json:"T"`
	Flag      Flag  `json:"F"`
	Value     Value `json:"V"`
}

// MakeTimestampFrom - convert d to dariadb timestamp format
func MakeTimestampFrom(d time.Time) Time {
	return Time(d.UnixNano() / int64(time.Millisecond))
}

// MakeTimestamp - return current time in dariadb format
func MakeTimestamp() Time {
	return MakeTimestampFrom(time.Now())
}

// Dariadb http client
type Dariadb struct {
	client   *http.Client
	hostname string
}

// New - create dariadb client
func New(host string) *Dariadb {
	return &Dariadb{
		client: &http.Client{}, hostname: host}
}

// Append value to storage
func (db *Dariadb) Append(param string, t Time, f Flag, v Value) error {

	measValue := map[string]interface{}{"F": f, "I": param, "T": t, "V": v}
	values := map[string]interface{}{
		"type": "append", "append_value": measValue}

	jsonValue, err := json.Marshal(values)

	if err != nil {
		return err
	}

	resp, err := db.client.Post(db.hostname, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		bodyContent, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(bodyContent))
	}
	return nil
}

func resp2scheme(resp *http.Response) (map[string]ID, error) {
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}
	result := make(map[string]ID)
	err = json.Unmarshal(body, &result)
	return result, err
}

// AddToScheme - add params to scheme
func (db *Dariadb) AddToScheme(params []string) (map[string]ID, error) {
	values := map[string]interface{}{"type": "scheme", "add": params}
	jsonValue, _ := json.Marshal(values)

	resp, err := db.client.Post(db.hostname, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return resp2scheme(resp)
}

// Scheme - load param list from server
func (db *Dariadb) Scheme() (map[string]ID, error) {
	resp, err := db.client.Get(db.hostname + "/scheme")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return resp2scheme(resp)
}

func json2result(js []byte) (map[string][]Measurement, error) {
	result := make(map[string][]Measurement)
	err := json.Unmarshal(js, &result)
	return result, err
}

func resp2result(resp *http.Response) (map[string][]Measurement, error) {
	if resp.StatusCode != 200 {
		bodyContent, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(bodyContent))
	}
	bodyContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return json2result(bodyContent)
}

// Interval - read interval
func (db *Dariadb) Interval(params []string, from Time, to Time, f Flag) (map[string][]Measurement, error) {
	values := map[string]interface{}{
		"type": "readInterval", "flag": f, "from": from, "to": to, "id": params}

	jsonValue, err := json.Marshal(values)

	if err != nil {
		return nil, err
	}

	resp, err := db.client.Post(db.hostname, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return resp2result(resp)
}

// Timepoint - read timepoint
func (db *Dariadb) Timepoint(params []string, timepoint Time, f Flag) (map[string][]Measurement, error) {
	values := map[string]interface{}{
		"type": "readTimepoint", "flag": f, "time": timepoint, "id": params}

	jsonValue, err := json.Marshal(values)

	if err != nil {
		return nil, err
	}

	resp, err := db.client.Post(db.hostname, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return resp2result(resp)
}
