package main

import (
	"log"
	"net"
	"os"
	"strconv"
	"context"

	pb "github.com/Miniim98/MandatoryExercise2/proto"
	"google.golang.org/grpc"
)

type Node struct {
	id int
	port string
	pb.UnimplementedDMEServer
}

var queue []Node
var network []Node
var this Node

func main() {
	var err error
	this.id, err = strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("first argument should be an integer ")
		os.Exit(1)
	}

	SetUpLog()
	connectToNetwork()
	go listen()

	for  i := 0; i <100000; i++ {
		//sendAccessRequest()
		//listen for others sending accessrequests
	}
}

//func sendAccessRequest(c pb.dMEClient) {}

func (node *Node) requestAccess (ctx context.Context, in *pb.AccesRequest) (*pb.AccesRequest, error) {
	
	return nil, nil
}

func sendResponse() {}

func listen() {
	for {
		for  i := 0; i <len(network) ; i++ {
			lis, err := net.Listen("tcp", network[i].port)
			if err != nil {
				log.Fatalf("failed to listen on port: " + network[i].port, err)
			}
	
			grpcServer := grpc.NewServer()
	
			pb.RegisterDMEServer(grpcServer, network[i])
	
			if err := grpcServer.Serve(lis); err != nil {
				log.Fatalf("failed to serve on port: " + network[i].port, err)
			}
		}
	}
}

func connectToNetwork() {
	for  i := 2; i <len(os.Args) ; i++ {
		var node Node
		node.port = os.Args[i]
		node.id = i-2
		network = append(network, node)
	}
	this.port = network[this.id].port
}

func SetUpLog() {
	var filename = "log" + strconv.Itoa(this.id)
	LOG_FILE := filename
	logFile, err := os.OpenFile(LOG_FILE, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
}

