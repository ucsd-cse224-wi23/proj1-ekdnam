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

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	_ = yaml.Unmarshal(f, &scs)

	return scs
}

func connectionHandler(conn net.Conn, ch chan<- []byte, maxMessageSize int) {
	record := make([]byte, maxMessageSize)
	n, err := conn.Read(record)
	if err != nil {
		log.Print("Error reading bytes from connection ", err)
	}
	record = record[:n]
	ch <- record
}

func recordListener(ch chan<- []byte, host string, port string, maxMessageSize int) {
	service := host + ":" + port
	listener, err := net.Listen("tcp", service)
	if err != nil {
		log.Println("NET.LISTEN ERROR", err)
	}
	defer listener.Close()
	for {
		nextConn, err := listener.Accept()
		if err != nil {
			log.Println("CONNECTION ACCEPT ERROR", err)
		}
		go connectionHandler(nextConn, ch, maxMessageSize)
	}
}

func recordSender(host string, port string, record []byte, sleepTime time.Duration, maxRetries int) {
	var clientConnection net.Conn
	var err error
	service := host + ":" + port
	for i := 0; i < maxRetries; i++ {
		clientConnection, err = net.Dial("tcp", service)
		if err != nil {
			log.Println("DIALING ERROR IN SENDRECORD()", err)
			time.Sleep(sleepTime * time.Millisecond)
		} else {
			break
		}
	}
	defer clientConnection.Close()

	_, err = clientConnection.Write(record)
	if err != nil {
		log.Println("WRITING ERROR IN SENDRECORD()", err)
	}
}

func getTotalServers(configPath string) int {
	scs := readServerConfigs(configPath)
	return len(scs.Servers)
}

func readAndSend(serverId int, bitsRequired int, readPath string, scs ServerConfigs, sleepTime time.Duration, MaxRetries int) [][]byte {
	records := [][]byte{}
	readFile, err := os.Open(readPath)
	if err != nil {
		fmt.Println("Error in opening file")
	}
	for {
		record := make([]byte, 100)
		_, err := readFile.Read(record)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("File read error in for loop ", err)
		}
		serverIdToWhichDataBelongs := int(record[0] >> (8 - bitsRequired))
		if serverIdToWhichDataBelongs == serverId {
			records = append(records, record)
			continue
		}
		for _, server := range scs.Servers {
			if serverIdToWhichDataBelongs == server.ServerId {
				recordSender(server.Host, server.Port, record, sleepTime, MaxRetries)
				break
			}
		}
	}
	readFile.Close()

	return records
}

func checkIfEnd(msg []byte, streamComplete []byte) bool {
	if len(msg) == len(streamComplete) {
		for i := range msg {
			if msg[i] != streamComplete[i] {
				return false
			}
		}
	} else {
		return false
	}
	return true
}

func receiveRecords(ch chan []byte, numberOfServers int, streamComplete []byte) [][]byte {
	records := [][]byte{}
	recvDataServers := 0
	for {
		if recvDataServers == numberOfServers-1 {
			break
		}
		msg := <-ch
		breakFlag := true

		breakFlag = checkIfEnd(msg, streamComplete)

		if breakFlag {
			recvDataServers += 1
		} else {
			records = append(records, msg)
		}
	}
	return records
}

func writeToFile(writePath string, records [][]byte) {
	writeFile, err := os.Create(writePath)
	if err != nil {
		log.Println("ERROR WHILE OPENING WRITE-ONLY FILE IN WRITETOFILE()", err)
	}

	for _, record := range records {
		writeFile.Write(record)
	}

	err = writeFile.Close()
	if err != nil {
		log.Println("ERROR WHILE CLOSING WRITE-ONLY FILE IN WRITETOFILE()", err)
	}
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

	ch := make(chan []byte)

	Host := scs.Servers[serverId].Host
	Port := scs.Servers[serverId].Port
	MaxMsgSize := 100
	sleepTime := time.Duration(100)
	MaxRetries := 5

	go recordListener(ch, Host, Port, MaxMsgSize)

	time.Sleep(sleepTime * time.Millisecond)

	readPath := os.Args[2]

	if err != nil {
		log.Println("Error opening file: ", err)
	}

	numberOfServers := getTotalServers(os.Args[4])
	bitsRequired := int(math.Log2(float64(numberOfServers)))

	myRecords := readAndSend(serverId, bitsRequired, readPath, scs, sleepTime, MaxRetries)

	streamComplete := []byte{}
	for i := 0; i < MaxMsgSize; i++ {
		streamComplete = append(streamComplete, 0)
	}
	for _, server := range scs.Servers {
		recordSender(server.Host, server.Port, streamComplete, sleepTime, MaxRetries)
	}

	recdRecords := receiveRecords(ch, numberOfServers, streamComplete)

	myRecords = append(myRecords, recdRecords...)

	sort.Slice(myRecords, func(i, j int) bool { return string(myRecords[i][:10]) < string(myRecords[j][:10]) })

	writeToFile(os.Args[3], myRecords)

}
