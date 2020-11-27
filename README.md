# Naive MapReduce

Yet another naive MapReduce, which runs locally using goroutine for simulation,
is written to complete the assignment of Cloud Computing. 

The BFS algorithm required by the assignment is in [here](https://github.com/unbyte/naive-mapreduce/blob/master/example/bfs/main.go).

To preview the result of algorithm, please clone this repository and run `make bfs`.

## Usage

```go
// MapReduce runs MapReduce locally using goroutine
//
// - should use `for-range` to read data inside mapper and reducer.
//
// - input should be closed after all data are sent.
//
// - poolSize is the number of mapper runners.
func MapReduce(mapper Mapper, reducer Reducer, input Input, output Output, poolSize int)
```

see codes under `/example` for additional information.

## LICENSE

MIT License.