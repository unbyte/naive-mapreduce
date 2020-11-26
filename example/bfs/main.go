package main

import (
	"encoding/json"
	"fmt"
	mapreduce "github.com/unbyte/naive-mapreduce"
	"io/ioutil"
	"reflect"
)

type Node struct {
	Label     string             `json:"label"`
	Distance  float64            `json:"distance"`
	Neighbors map[string]float64 `json:"neighbors"`
}

type Result = map[string]float64

func mapper(input mapreduce.Input, output mapreduce.Producer) {
	for i := range input {
		node := i.(Node)
		output <- mapreduce.Data{
			Key:   node.Label,
			Value: node.Distance,
		}
		for label, distance := range node.Neighbors {
			output <- mapreduce.Data{
				Key:   label,
				Value: distance + node.Distance,
			}
		}
	}
}

func reducer(input mapreduce.Receiver, output mapreduce.Output) {
	result := make(Result)
	for data := range input {
		key := data.Key.(string)
		value := data.Value.(float64)
		oldValue, ok := result[key]
		if !ok || oldValue > value {
			result[key] = value
		}
	}
	output <- result
}

func mapEqual(a, b Result) bool {
	return reflect.DeepEqual(a, b)
}

func readGraph(path string) []Node {
	description, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err.Error())
	}
	nodes := make([]Node, 0, 8)

	if err = json.Unmarshal(description, &nodes); err != nil {
		panic(err.Error())
	}
	return nodes
}

func generateInput(nodes []Node) mapreduce.Input {
	inputPipe := make(chan interface{})
	go func() {
		for _, input := range nodes {
			inputPipe <- input
		}
		close(inputPipe)
	}()
	return inputPipe
}

func updateNodes(nodes []Node, distances Result) {
	for i := range nodes {
		nodes[i].Distance = distances[nodes[i].Label]
	}
}

func main() {
	nodes := readGraph("./testdata/graph.json")
	output := make(chan interface{})
	var result Result
	for {
		mapreduce.MapReduce(mapper, reducer, generateInput(nodes), output, 8)
		newResult := (<-output).(Result)
		if mapEqual(result, newResult) {
			break
		}
		result = newResult
		updateNodes(nodes, result)
	}
	fmt.Printf("%+v\n", result)
}
