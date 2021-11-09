package main

import (
	"os"
	"log"
	"google.golang.org/grpc"
	pb "github.com/Miniim98/MandatoryExercise2/proto"
)

type Node struct {
	int id
	//port Address

}

func main() {
	SetUpLog()

	for  i := 0; i <100000; i++ {
		sendAccessRequest()
		//listen for others sending accessrequests
	}
}

func sendAccessRequest(c pb.dMEClient) {

}

func sendAccesResponse() {}

func SetUpLog() {
	var filename = "log " + os.Args[0]
	LOG_FILE := filename
	logFile, err := os.OpenFile(LOG_FILE, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
}

