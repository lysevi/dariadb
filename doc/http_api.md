# Write single value

send POST query:
```json
{
 "append_value": {
  "F": 0,
  "I": "single_value",
  "T": 1491207386162,
  "V": 777.0
 },
 "type": "append"
}
```
where "F" - flag, "I" - measurement name, "T" - time as count of milliseconds since from zero-time, "V" - value.

# Write many values

send POST query:
```json
{
 "append_values": {
 "cpu1": {
   "F": [ 0, 10, 20, 30, 40, 50,  60,  70,  80,  90  ],
   "T": [ 0, 10, 20, 30, 40, 50,  60,  70,  80,  90  ],
   "V": [ 0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0]
  },
  "memoryUsage": {
   "F": [ 1, 11, 21, 31, 41, 51, 61, 71, 81, 91 ],
   "T": [ 1, 11, 21, 31, 41, 51, 61, 71, 81, 91 ],
   "V": [ 1.0, 11.0, 21.0, 31.0, 41.0, 51.0, 61.0, 71.0, 81.0,  91.0 ]
  }
 },
 "type": "append"
}
```
where "cpu1", "memoryUsage" - measurements names, "F" - array of flags, "T" - array of times, "V" - array of values.

# Read interval

send POST query:
```json
{
 "flag": 0,
 "from": 0,
 "id": ["cpu1", "memory", "network" ],
 "to": 18446744073709551615,
 "type": "readInterval"
}
```
where "from", "to" - time interval as count of milliseconds since from zero-time, "id" - measurements.

answer example:

```json
{
 "cpu1": [{"F": 0,"T": 0,"V": 0.0 } ],
 "memory": [{"F": 1,"T": 1,"V": 1.0 }, { "F": 11, "T": 11, "V": 11.0 }],
 "network": [{"F": 777,"T": 1491209163670,"V": 777.0}]
}
```

# Read timepoint

send POST query:
```json
{
 "flag": 0,
 "id": [ "cpu1", "memory", "network" ],
 "time": 18446744073709551615,
 "type": "readTimepoint"
}
```
answer example:

```json
{
 "cpu1": [{"F": 0,"T": 0,"V": 0.0 } ],
 "memory": [{ "F": 11, "T": 11, "V": 11.0 }],
 "network": [{"F": 777,"T": 1491209163670,"V": 777.0}]
}
```

# Read scheme

send GET query to URL "http://dariadb_host:port/scheme". Result example:

```json
{"cpu":0, "memory":1,"network":2, "single_value":10}
```
is a dictinary, where name is a key, and id as value.

# Add new param to scheme
send POST query:
```json
{
 "add": ["new1","new2","new3"],
 "type": "scheme"
}
```
where "new1", "new2", "new3" - names of new params

answer example:

```json
{
 "new1": 11,
 "new2": 12,
 "new3": 13
}
```
is a dictinary, where "key" - param name, and value - param id.

# Read statistic for value
send POST query:
```json
{
 "from": 0,
 "id": "cpu1",
 "to": 18446744073709551615,
 "type": "stat"
}
```
where "from", "to" - time, "id" - measurement name.

answer example:

```json
{
 "cpu1": {
  "count": 1,
  "maxTime": 1491209163670,
  "maxValue": 777.0,
  "minTime": 1491209163670,
  "minValue": 777.0,
  "sum": 777.0
 }
}
```

# Query available statistic functions

send GET query to URL "http://dariadb_host:port/statfuncs". Result example:

```json
{
 "functions": [
  "average",
  "median",
  "percentile90",
  "percentile99",
  "sigma"
 ]
}
```

# Query statistic calculation

```json
{
 "type": "statistic",
 "flag": 0,
 "from": 0,
 "id": "memory",
 "to": 18446744073709551615,
 "functions": [
  "average",
  "median",
  "percentile90"
 ]
}

```
where from,to,flag, id - query interval params, funcstions - statistical functions to calculation. Result example:

```json
{
 "measurement": "memory",
 "average": {
  "F": 0,
  "T": 0,
  "V": 777.0
 },
 "median": {
  "F": 0,
  "T": 0,
  "V": 700.0
 },
 "percentile90": {
  "F": 0,
  "T": 0,
  "V": 700.0
 }
}

```

# Erase old value older than time

send post query

``` json
{
 "id": "memory",
 "to": 18446744073709551615,
 "type": "erase"
}
```
where "to" is time point, all values older than  will be removed.
