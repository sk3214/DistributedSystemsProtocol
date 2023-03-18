package cos418_hw1_1

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	// fmt.Println("Inside sumWorker")
	sum := 0
	for len(nums) > 0 {
		sum += <-nums
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	chansInput := make(chan int, 10)
	chansOutput := make(chan int, 1)
	res := 0
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("There is an error in opening of file %s", err)
	}
	numsArray, err := readInts(file)
	if err != nil {
		log.Printf("There is an error in retrieving array from readInts %s", err)
	}
	for _, num := range numsArray {
		if len(chansInput) == cap(chansInput) {
			go sumWorker(chansInput, chansOutput)
			res += <-chansOutput
		}
		chansInput <- num
	}
	if len(chansInput) > 0 {
		go sumWorker(chansInput, chansOutput)
		res += <-chansOutput
	}
	return res
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
