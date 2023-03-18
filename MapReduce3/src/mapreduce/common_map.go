package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
type fileObject struct {
	f *os.File
	j *json.Encoder
}

func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//

	// Remember to close the file after you have written all the values!
	// Use checkError to handle errors.
	// fmt.Println("inFile ", inFile)
	fileBytes, err := os.ReadFile(inFile)
	if err != nil {
		log.Fatalf("Some error in opening the file %s", err)
	}
	fileContent := string(fileBytes)
	mapValues := mapF(inFile, fileContent)
	fileObjectArray := make([]fileObject, 0, nReduce)

	// ind := nReduce
	for ind := 0; ind < nReduce; ind++ {
		fileName := reduceName(jobName, mapTaskNumber, ind)
		// fmt.Println("fileName is", fileName)
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
		if err != nil {
			log.Fatalf("File could not be created %s", err)
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		fileObjectArray = append(fileObjectArray, fileObject{file, enc})
	}
	for _, kv := range mapValues {
		index := kv.Key
		hashedIndex := ihash(index)
		err := fileObjectArray[hashedIndex%uint32(nReduce)].j.Encode(&kv)
		if err != nil {
			fmt.Println("Error in map phase", err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
