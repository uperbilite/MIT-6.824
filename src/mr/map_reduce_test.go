package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}

func Sequential(inFileName string, outFileName string,
	mapF func(string, string) []KeyValue,
	reduceF func(key string, values []string) string) {

	file, err := os.Open(inFileName)
	if err != nil {
		log.Fatalf("cannot open %v", inFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inFileName)
	}
	file.Close()

	var intermediate []KeyValue
	kva := mapF(inFileName, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))
	outFile, _ := os.Create(outFileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceF(intermediate[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	outFile.Close()
}
