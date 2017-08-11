package mapreduce

import (
	"fmt"
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
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
	//下面总的来说就是建议我们用jason来在文件中保存key-value
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
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//从这里开始写
	file, err := os.Open(inFile) // For read access.
	fmt.Println(inFile)
	if err != nil {
		log.Fatal("doMap failed to open input file: ", inFile, " error: ", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		log.Fatal("get input file info failed: ", file, " error: ", err)
	}

	data := make([]byte, fi.Size())
	_, err = file.Read(data)
	if err != nil {
		log.Fatal("read inpufile failed: ", inFile, " error: ", err)
	}

	kv := mapF(fi.Name(), string(data))

	rFiles := make([]*os.File, nReduce)
	rEncodes := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ { //创建rReduce个中间文件供reduce步骤读取
		rFileName := reduceName(jobName, mapTaskNumber, i)
		rFile, err := os.Create(rFileName)
		if err != nil {
			log.Fatal("create middle file failed: ", err)
		}
		defer rFile.Close()
		rFiles[i] = rFile
		defer rFiles[i].Close()
		rEncodes[i] = json.NewEncoder(rFiles[i])
	}

	for _, v := range kv { //遍历经过mapF函数处理得到的中间键值对，分别写入对应的中间文件
		n := ihash(v.Key) % uint32(nReduce)
		err = rEncodes[n].Encode(v)
		if err != nil {
			log.Fatal("encode kv failed: ", err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
