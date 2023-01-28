package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

const (
	Proto      = "tcp"
	MaxMsgSize = 100
	SleepTime  = time.Duration(100) * time.Millisecond
)

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func sendRecord(conn net.Conn, record []byte) {
	// writing record to connection
	_, err := conn.Write(record)
	if err != nil {
		log.Fatalln("Error while sending record ", err)
	}
}

func startServer(Host string, Port string, ch chan<- []byte) {
	service := Host + ":" + Port
	// starting server by listening using Proto TCP
	listener, err := net.Listen(Proto, service)
	if err != nil {
		fmt.Println("Error while listening")
	}
	defer listener.Close()
	for {
		// accepting on listener
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error occurred while accepting connection ", err)
		}
		go handleConnection(conn, ch)
	}
}

func handleConnection(conn net.Conn, ch chan<- []byte) {
	for {
		record := make([]byte, 100)
		// reading from connection into record
		length, err := conn.Read(record)
		if err != nil {
			if err == io.EOF {
				fmt.Println("All data has been read, exiting ", err)
				return
			}
			fmt.Println("Error occurred ", err)
		}
		// read first length bytes of record into record
		record = record[:length]
		// read record into channel
		ch <- record
	}
}

func binningData(numberOfServers int, readFile []byte, bits int) map[int][]byte {
	dataBucket := make(map[int][]byte)

	i := 0

	for {
		if !(i < len(readFile)) {
			break
		}
		data := readFile[i : i+100]
		i += 100
		sendServerId := int(data[0] >> (8 - bits))
		for _, b := range data {
			dataBucket[sendServerId] = append(dataBucket[sendServerId], b)
		}
	}

	for id := 0; id < numberOfServers; id++ {
		for i := 0; i < 100; i++ {
			dataBucket[id] = append(dataBucket[id], byte(0))
		}
	}

	return dataBucket
}

func receiveRecords(ch chan []byte, numberOfServers int) [][]byte {
	var records [][]byte
	serversCompleted := 0

	for {
		if serversCompleted == numberOfServers {
			break
		}
		clientDataProcessing := true
		record := <-ch
		for _, byte := range record {
			if byte != 0 {
				clientDataProcessing = false
				break
			}
		}
		if clientDataProcessing {
			serversCompleted += 1
			continue
		} else {
			records = append(records, record)
		}
	}
	return records
}

func createConnectionMap(connectionMap map[int]net.Conn, scs ServerConfigs, numberOfServers int) map[int]net.Conn {
	for i := 0; i < numberOfServers; i++ {
		for {
			clientHost := scs.Servers[i].Host
			clientPort := scs.Servers[i].Port
			service := clientHost + ":" + clientPort
			connection, err := net.Dial(Proto, service)
			if err != nil {
				fmt.Println("Error while setting up connection ", err)
				time.Sleep(SleepTime)
				continue
			} else {
				connectionMap[i] = connection
				break
			}
		}
	}
	return connectionMap
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/
	connectionMap := make(map[int]net.Conn)

	Host := scs.Servers[serverId].Host
	Port := scs.Servers[serverId].Port

	numberOfServers := len(scs.Servers)
	bits := int(math.Log2(float64(numberOfServers)))

	ch := make(chan []byte)

	// start server
	go startServer(Host, Port, ch)
	time.Sleep(SleepTime)

	// setting up connection to all servers
	connectionMap = createConnectionMap(connectionMap, scs, numberOfServers)

	readPath := os.Args[2]
	readFile, err := os.ReadFile(readPath)
	if err != nil {
		fmt.Println("Error while reading file ", err)
	}

	dataBucket := binningData(numberOfServers, readFile, bits)

	for i := 0; i < numberOfServers; i++ {
		_, err := connectionMap[i].Write(dataBucket[i])
		if err != nil {
			fmt.Println("Error while writing data to connection")
		}
	}

	records := receiveRecords(ch, numberOfServers)

	sort.Slice(records, func(i, j int) bool {
		return string(records[i][:10]) < string(records[j][:10])
	})

	writePath := os.Args[3]
	writeFile, err := os.Create(writePath)
	if err != nil {
		fmt.Println("Error occurred while opening write file")
	}
	for _, record := range records {
		writeFile.Write(record)
	}
	writeFile.Close()
	for _, v := range connectionMap {
		v.Close()
	}

}
