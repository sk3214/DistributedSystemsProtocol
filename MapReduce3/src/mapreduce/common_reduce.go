package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
	// fmt.Println("nMap is", nMap)
	resultMap := make(map[string][]string)
	for ind := 0; ind < nMap; ind++ {
		fileName := reduceName(jobName, ind, reduceTaskNumber)
		// fmt.Println("filename using reduceName is", fileName)
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
		// fmt.Println("File name and contents ", file.Name())
		if err != nil {
			log.Fatalf("File could not be read %s", err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("Error in decoding %s", err)
			}
			resultMap[kv.Key] = append(resultMap[kv.Key], kv.Value)
		}
	}
	// fmt.Println("ResultMap = ", resultMap)
	var keys []string

	for k := range resultMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// fmt.Println("keys = ", keys)
	mergeFile := mergeName(jobName, reduceTaskNumber)
	outputFile, err := os.OpenFile(mergeFile, os.O_CREATE|os.O_RDWR, 0755)
	// fmt.Printf("The output file is %s", outputFile.Name())
	if err != nil {
		log.Fatalf("Some error in creating the output file")
	}
	enc := json.NewEncoder(outputFile)
	for _, k := range keys {
		result := reduceF(k, resultMap[k])
		// fmt.Printf("The result is %s", result)
		// fmt.Println()
		enc.Encode(KeyValue{k, result})
	}
	defer outputFile.Close()

}
