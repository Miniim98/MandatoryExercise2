package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	//"github.com/hashicorp/serf/serf"
	//"github.com/pkg/errors"

	pb "github.com/Miniim98/MandatoryExercise2/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Node struct {
	id   int
	Addr string
	// Consul related variables
	SDAddress string
	SDKV      api.KV

	// used to make requests
	Clients map[int]pb.DMEClient
	pb.UnimplementedDMEServer
}

type timestamp struct {
	time int32
	mu   sync.Mutex
}

var Time timestamp

func (c *timestamp) UpTimestamp() {
	c.mu.Lock()
	defer c.mu.Unlock()
	Time.time++
}

//var queue []serf.Member
//var network []serf.Member
var state string

func main() {
	Time.time = 0
	//var err error
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Println("Arguments required: <id> <listening address> <consul address>")
		os.Exit(1)
	}

	// args in order
	id, err := strconv.Atoi(args[0])
	listenaddr := args[1]
	sdaddress := args[2]

	if err != nil {
		fmt.Println("id must be an integer")
		os.Exit(1)
	}

	n := Node{id: id, Addr: listenaddr, SDAddress: sdaddress, Clients: nil}
	n.SetUpLog()

	n.Clients = make(map[int]pb.DMEClient)
	go n.listen()
	fmt.Println("Listen")
	n.registerService()
	fmt.Println("Register")

	for i := 0; i < 10; i++ {

		n.sendRequestAccess()
		time.Sleep(10 * time.Second)
		//listen for others sending accessrequests
	}

	for {
	}
}

func (n *Node) sendRequestAccess() {
	fmt.Println("sendRequest")
	state = "WANTED"
	noOfResponse := 0

	kvpairs, _, err := n.SDKV.List("", nil)
	if err != nil {
		log.Println(err)
		return
	}

	for _, kventry := range kvpairs {
		key, err := strconv.Atoi(kventry.Key)
		if err != nil {
			log.Fatal("A key is in the wrong format")
			os.Exit(1)
		}

		if n.id == key {
			continue
		}
		if n.Clients[key] == nil {
			fmt.Println("New member: ", key)
			// connection not established previously
			n.SetupClientToRequest(key, string(kventry.Value))
		}
	}

	if len(n.Clients) > 0 {
		for _, member := range n.Clients {
			message := pb.AccesRequest{Timestamp: &pb.Timestamp{Events: Time.time}, RequestingId: int32(n.id)}
			response, err := member.RequestAccess(context.Background(), &message)

			if err != nil {
				log.Fatalf("Error when calling RequestAccess: %s", err)
			}
			if response.ResponseGranted {
				noOfResponse++
			}
			fmt.Println(response)

		}
	}
	if noOfResponse == len(n.Clients) {
		state = "HELD"
	}

}

func (n *Node) RequestAccess(ctx context.Context, in *pb.AccesRequest) (*pb.AccessResponse, error) {
	if state == "HELD" || (state == "WANTED" && (Time.time < in.Timestamp.Events)) {
		fmt.Println("queue")
	}
	return &pb.AccessResponse{Timestamp: &pb.Timestamp{Events: Time.time}, ResponseGranted: false}, nil
}

func (n *Node) listen() {
	lis, err := net.Listen("tcp", n.Addr)
	if err != nil {
		log.Fatalf("failed to listen on port: "+n.Addr, err)
	}

	_n := grpc.NewServer()
	fmt.Println("listening on port: " + n.Addr)
	pb.RegisterDMEServer(_n, n)

	reflection.Register(_n)
	if err := _n.Serve(lis); err != nil {
		log.Fatalf("failed to serve on port: "+n.Addr, err)
	}

}

func (n *Node) SetUpLog() {
	var filename = "log" + strconv.Itoa(n.id)
	LOG_FILE := filename
	logFile, err := os.OpenFile(LOG_FILE, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
}

func (n *Node) registerService() {
	fmt.Println("Reached register")
	config := api.DefaultConfig()
	config.Address = n.SDAddress
	consul, err := api.NewClient(config)
	if err != nil {
		log.Println("Unable to contact Service Discovery.")
	}
	kv := consul.KV()
	p := &api.KVPair{Key: strconv.Itoa(n.id), Value: []byte(n.Addr)}
	_, err = kv.Put(p, nil)
	if err != nil {
		log.Panicf("Unable to register with Service Discovery. error : %v", err)
	}

	// store the kv for future use
	n.SDKV = *kv

	log.Println("Successfully registered with Consul.")
}

// Setup a new grpc client for contacting the server at addr.
func (n *Node) SetupClientToRequest(id int, addr string) {

	// setup connection with other node
	conn, err := grpc.Dial("127.0.0.1"+addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	n.Clients[id] = pb.NewDMEClient(conn)

	r, err := n.Clients[id].RequestAccess(context.Background(), &pb.AccesRequest{Timestamp: &pb.Timestamp{Events: Time.time}, RequestingId: int32(n.id)})
	if err != nil {
		log.Fatalf("could not Request: %v", err)
	}
	log.Printf("Greeting from the other node: %t", r.ResponseGranted)

}
