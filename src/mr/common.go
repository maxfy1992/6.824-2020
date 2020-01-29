package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

// Debugging enabled?
const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

//
// Map functions return a slice of KeyValue.
//
// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	// 2018 diff
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
	//return "mr-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	// 2018 diff
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
	//return "mr-out-" + strconv.Itoa(reduceTask)
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	file, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("doMap: ", err)
	}
	outputFiles := make([]*os.File, nReduce) // create intermediate files
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTask, i)
		outputFiles[i], err = os.Create(fileName)
		if err != nil {
			log.Fatal("Error in creating file: ", fileName)
		}
	}

	keyValuePairs := mapF(inFile, string(file))
	for _, kv := range keyValuePairs {
		index := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(outputFiles[index])
		enc.Encode(kv)
	}
	for _, file := range outputFiles {
		file.Close()
	}
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	inputFiles := make([]*os.File, nMap)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		inputFiles[i], _ = os.Open(fileName)
	}

	// collect key/value pairs from intermediate files
	intermediateKeyValues := make(map[string][]string)
	for _, inputFile := range inputFiles {
		defer inputFile.Close()
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediateKeyValues[kv.Key] = append(intermediateKeyValues[kv.Key], kv.Value)
		}
	}
	keys := make([]string, 0, len(intermediateKeyValues))
	for k := range intermediateKeyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// writer reduce out to file
	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal("Error in creating file", outFile)
	}
	defer out.Close()

	// 2018 diff
	/*
		w := bufio.NewWriter(out)
		for _, key := range keys {
			kv := KeyValue{key, reduceF(key, intermediateKeyValues[key])}
			fmt.Fprintf(w, "%v %v\n", key, kv.Value)
		}
		w.Flush()
	*/

	enc := json.NewEncoder(out)
	for _, key := range keys {
		kv := KeyValue{key, reduceF(key, intermediateKeyValues[key])}
		enc.Encode(kv)
	}

}
