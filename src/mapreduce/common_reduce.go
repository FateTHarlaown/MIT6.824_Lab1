package mapreduce

import (
	"encoding/json"
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
	KeyValues := make(map[string]string)

	for i := 0; i < nMap; i++ { //循环读取每个中间文件中的键值对到KeyValues中，先不考虑内存是否足够的问题
		rFileName := reduceName(jobName, i, reduceTaskNumber)
		rFile, err := os.Open(rFileName)
		if err != nil {
			log.Fatal("open the intermediate file: ", rFileName, "failed error: ", err)
		}

		for { //从一个中间文件中循环读取出键值对，并将不重复的键值对存储到KeyValus中, 丢弃后面的重复键值对。读取一直到文件结束。
			var kv KeyValue
			deccoder := json.NewDecoder(rFile)

			err = deccoder.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := KeyValues[kv.Key]
			if !ok {
				KeyValues[kv.Key] = kv.Value
			}
		}
	}

	keys := make([]string, 0)
	for k, _ := range KeyValues {
		keys = append(keys, k)
	}

	sort.Strings(keys)
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
}
