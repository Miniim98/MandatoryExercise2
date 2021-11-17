package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"

	pb "github.com/Miniim98/MandatoryExercise2/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Node struct {
	id    int
	Addr  string
	state State
	// Consul related variables:
	SDAddress string
	SDKV      api.KV

	// used to make requests:
	Clients     map[int]pb.DMEClient
	streamQueue []pb.DME_RequestAccessServer
	pb.UnimplementedDMEServer
}

type timestamp struct {
	time int32
	mu   sync.Mutex
}

type State struct {
	st string
	mu sync.Mutex
}

var Time timestamp

func (c *timestamp) UpTimestamp() {
	c.mu.Lock()
	defer c.mu.Unlock()
	Time.time++
}

func (n *Node) readWriteState(str string) *State {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	if str != "read" {
		n.state.st = str
	}
	return &n.state

}

func main() {
	Time.time = 0
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

	n := Node{id: id, Addr: listenaddr, state: State{st: "RELEASED"}, SDAddress: sdaddress, Clients: nil}
	n.SetUpLog()

	n.Clients = make(map[int]pb.DMEClient)
	go n.listen()
	fmt.Println("I have started listening")
	n.registerService()
	fmt.Println("I have registered")
	go n.checkForNewMembers()
	n.sendRequestAccess()

}

func (n *Node) checkForNewMembers() {
	for {
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
				fmt.Println("I have found a new member: ", key)
				log.Println("I have found a new memeber: ", key)
				n.SetupClientToRequest(key, string(kventry.Value))
			}
		}
		time.Sleep(2 * time.Second)
	}

}

func (n *Node) sendRequestAccess() {
	for {
		if n.readWriteState("read").st != "HELD" {
			time.Sleep(5 * time.Second)
			if len(n.Clients) > 0 {

				Time.UpTimestamp()
				fmt.Println("sending request")
				n.readWriteState("WANTED")
				noOfResponse := 0

				clients := len(n.Clients)
				for _, member := range n.Clients {
					log.Println("Sending request")
					Time.UpTimestamp()
					message := pb.AccesRequest{Timestamp: &pb.Timestamp{Events: Time.time}, RequestingId: int32(n.id)}
					stream, err := member.RequestAccess(context.Background())
					if err != nil {
						log.Fatalf("Error when calling RequestAccess: %s", err)
					}
					if err := stream.Send(&message); err != nil {
						log.Printf("Error sending stream to Request acces : %v", err)
					}
					in, err := stream.Recv()
					if err != nil {
						log.Printf("i expected a message but did not receive one. exiting ERROR : %v", err)
						break
					}
					log.Printf("I recived response from %d \n", in.ResponseId)
					Time.UpTimestamp()
					if in.ResponseGranted {
						noOfResponse++
					}

				}
				log.Printf("i received %d responses and i have %d clients", noOfResponse, len(n.Clients))
				if noOfResponse == clients {
					n.readWriteState("HELD")
					log.Println("I HAVE THE KEY")
					file, _ := os.OpenFile("CriticalSection.txt", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
					io.WriteString(file, time.Now().Format(time.Stamp)+" "+strconv.Itoa(n.id)+" has the key \n")
					time.Sleep(3 * time.Second)
					n.releaseKey()
				}
			}

		}
		time.Sleep(2 * time.Second)

	}

}

func (n *Node) RequestAccess(stream pb.DME_RequestAccessServer) error {

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		Time.UpTimestamp()
		var first int

		if Time.time == in.Timestamp.Events {
			first = int(math.Min(float64(n.id), float64(in.RequestingId)))
		} else {
			if Time.time < in.Timestamp.Events {
				first = n.id
			} else {
				first = int(in.RequestingId)
			}
		}
		log.Printf("got request from %d. My state is %s", in.RequestingId, n.state.st)

		if n.readWriteState("read").st == "HELD" || (n.readWriteState("read").st == "WANTED" && first == n.id) {
			n.streamQueue = append(n.streamQueue, stream)
		} else {
			Time.UpTimestamp()
			err := stream.Send(&pb.AccessResponse{Timestamp: &pb.Timestamp{Events: Time.time}, ResponseGranted: true, ResponseId: int32(n.id)})
			log.Printf("Sending response to %d ", in.RequestingId)
			if err != nil {
				log.Printf("Error when sending response Error : %v", err)
			}
		}
	}
}

func (n *Node) releaseKey() {
	if n.readWriteState("read").st == "HELD" {
		n.readWriteState("RELEASED")
		log.Println("I NO LONGER HAVE THE KEY")
		file, _ := os.OpenFile("CriticalSection.txt", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
		io.WriteString(file, time.Now().Format(time.Stamp)+" "+strconv.Itoa(n.id)+" no longer has the key\n")
		time.Sleep(time.Second)
		for i := 0; i < len(n.streamQueue); i++ {
			Time.UpTimestamp()
			err := n.streamQueue[i].Send(&pb.AccessResponse{Timestamp: &pb.Timestamp{Events: Time.time}, ResponseGranted: true, ResponseId: int32(n.id)})
			log.Printf("Sending response to queue %d", i)
			if err != nil {
				log.Printf("Error when sending response Error : %v", err)

			}
		}
		n.streamQueue = nil
	}
}

func (n *Node) listen() {
	for {
		lis, err := net.Listen("tcp", n.Addr)
		if err != nil {
			log.Fatalf("failed to listen on port: "+n.Addr, err)
		}
		var opts []grpc.ServerOption
		_n := grpc.NewServer(opts...)
		fmt.Println("listening on port: " + n.Addr)
		pb.RegisterDMEServer(_n, n)

		reflection.Register(_n)
		if err := _n.Serve(lis); err != nil {
			log.Fatalf("failed to serve on port: "+n.Addr, err)
		}
	}
}

func (n *Node) SetUpLog() {
	var filename = "log" + strconv.Itoa(n.id) + ".txt"
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

	n.Clients[id] = pb.NewDMEClient(conn)
	log.Printf("Added a client. Number of clients is now %d", len(n.Clients))

}
