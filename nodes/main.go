package main

import (
	"os"
	"log"
	"google.golang.org/grpc"
	pb "github.com/Miniim98/MandatoryExercise2/proto"
)

func main() {
	SetUpLog(id)

	for  i := 0; i <100000; i++ {
		requestAccess()
	}
}

func sendAccessRequest() {

}

func sendAccesResponse() {}

func SetUpLog(int id) {
	var filename = "log " + id
	LOG_FILE := filename
	logFile, err := os.OpenFile(LOG_FILE, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
}

