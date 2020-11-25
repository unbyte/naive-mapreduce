package main

import (
	"bufio"
	"fmt"
	mapreduce "github.com/unbyte/naive-mapreduce"
	"io"
	"os"
	"regexp"
)

var (
	wordsRE = regexp.MustCompile(`[A-Za-z0-9_\-]*`)
)

func mapper(input mapreduce.Input, output mapreduce.Producer) {
	for data := range input {
		result := make(map[string]int)
		for _, match := range wordsRE.FindAllString(data.(string), -1) {
			result[match]++
		}

		for k, v := range result {
			output <- mapreduce.Data{
				Key:   k,
				Value: v,
			}
		}
	}
}

func reducer(input mapreduce.Receiver, output mapreduce.Output) {
	result := make(map[string]int)
	for data := range input {
		result[data.Key.(string)] += data.Value.(int)
	}
	output <- result
}

func main() {
	input := make(chan interface{})
	output := make(chan interface{})
	mapreduce.MapReduce(mapper, reducer, input, output, 6)
	fi, err := os.Open("./testdata/bible.txt")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()

	br := bufio.NewReader(fi)
	for {
		line, err := br.ReadString('\n')
		input <- line
		if err == io.EOF {
			break
		}
	}
	close(input)
	result := <-output
	fmt.Printf("%+v\n", result)
}
