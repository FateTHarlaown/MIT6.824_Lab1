package mapreduce

import (
	"encoding/json"
	//"fmt"
	"log"
	"os"
	"sort"
)

type KV struct {
	Key   string
	Value string
}

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
	KeyValues := make(map[string][]string)

	//fmt.Println("Now get ", nMap, " rFiles")
	for i := 0; i < nMap; i++ { //循环读取每个中间文件中的键值对到KeyValues中，先不考虑内存是否足够的问题
		rFileName := reduceName(jobName, i, reduceTaskNumber)
		//rFileName := "mrtmp.test-0-0"
		rFile, err := os.Open(rFileName)
		if err != nil {
			log.Fatal("open the intermediate file: ", rFileName, "failed error: ", err)
		}

		defer rFile.Close()
		dec := json.NewDecoder(rFile)
		var kv KeyValue

		for { //从一个中间文件中循环读取出键值对，并将键值对存储到KeyValus中
			err := dec.Decode(&kv)
			//fmt.Println(kv)
			if err != nil {
				//fmt.Println("Error: ", err)
				break
			}
			//fmt.Println("reduce get a kv from json: ", kv, "the rFileName: ", rFileName)
			_, exis := KeyValues[kv.Key]
			if !exis { //没有这个key值则需要创建一个[]string的切片
				KeyValues[kv.Key] = make([]string, 0)
			}
			KeyValues[kv.Key] = append(KeyValues[kv.Key], kv.Value)
		}
	}
	//fmt.Println("KV MAP has: ", len(KeyValues))
	//keys中存放各个键值对的key，用于进行排序
	keys := make([]string, 0)
	for k, _ := range KeyValues {
		keys = append(keys, k)
	}
	//fmt.Println(len(keys))
	sort.Strings(keys)

	mFileName := mergeName(jobName, reduceTaskNumber)
	mFile, err := os.Create(mFileName)
	if err != nil {
		log.Fatal("can not ctreate ouput merge file: ", mFileName, "error: ", err)
	}
	defer mFile.Close()
	enc := json.NewEncoder(mFile)

	for _, k := range keys { //依次对各个key值调用用户提供的mapF函数并将结果写入merge文件
		var ans KeyValue
		ans.Value = reduceF(k, KeyValues[k])
		ans.Key = k
		err := enc.Encode(ans)
		if err != nil {
			log.Fatal("encode ans failed!:", ans, "error: ", err)
		}
	}
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
