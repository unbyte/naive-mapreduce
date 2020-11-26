package mapreduce

import (
	"sync"
)

type Data struct {
	Key   interface{}
	Value interface{}
}

type Pipe = chan Data
type Producer = chan<- Data
type Receiver = <-chan Data

type Input = <-chan interface{}
type Output = chan<- interface{}

type Mapper func(Input, Producer)
type Reducer func(Receiver, Output)

func mergePipes(pipes []Pipe) Receiver {
	out := make(Pipe, len(pipes))
	var wg sync.WaitGroup
	wg.Add(len(pipes))
	for _, pipe := range pipes {
		go func(r Receiver) {
			for v := range r {
				out <- v
			}
			wg.Done()
		}(pipe)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// MapReduce runs reduce map on local
//
// - should use `range over channel` inside mapper and reducer.
//
// - input should be closed after all data are sent.
//
// - poolSize is the number of mapper runners.
func MapReduce(mapper Mapper, reducer Reducer, input Input, output Output, poolSize int) {
	// input -> broker >>> pipes => mergedPipes -> output
	pipes := make([]Pipe, poolSize)
	for i := 0; i < poolSize; i++ {
		pipes[i] = make(Pipe)
	}

	go reducer(mergePipes(pipes), output)

	broker := make(chan interface{})
	for _, pipe := range pipes {
		go func(pipe Producer) {
			mapper(broker, pipe)
			close(pipe)
		}(pipe)
	}

	go func() {
		for data := range input {
			broker <- data
		}
		close(broker)
	}()
}
