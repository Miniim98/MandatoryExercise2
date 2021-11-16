package main

import (
<<<<<<< Updated upstream
	"os"
	"log"
=======
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"

	pb "github.com/Miniim98/MandatoryExercise2/proto"
>>>>>>> Stashed changes
	"google.golang.org/grpc"
	pb "github.com/Miniim98/MandatoryExercise2/proto"
)

type Node struct {
<<<<<<< Updated upstream
	int id
	//port Address

}

func main() {
	SetUpLog()

	for  i := 0; i <100000; i++ {
		sendAccessRequest()
=======
	id   int
	port string
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

var queue []Node
var network []Node
var this Node
var state string

func main() {
	Time.time = 0
	var err error
	this.id, err = strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("first argument should be an integer ")
		os.Exit(1)
	}

	SetUpLog()
	connectToNetwork()
	go listen()
	cluster, err := setupCluster(
		os.Getenv(this.port),
		os.Getenv("CLUSTER_ADDR"))
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.Leave()

	for i := 0; i < 100000; i++ {
		members := getOtherMembers(cluster)
		sendRequestAccess(members)
>>>>>>> Stashed changes
		//listen for others sending accessrequests
	}
}

<<<<<<< Updated upstream
func sendAccessRequest(c pb.dMEClient) {

}

func sendAccesResponse() {}
=======
//func sendAccessRequest(c pb.dMEClient) {}

func sendRequestAccess(otherMembers []serf.Member) {
	state = "WANTED"
	noOfResponse := 0
	if len(otherMembers) > 1 {
		for _, member := range otherMembers {
			var conn *grpc.ClientConn
			conn, err := grpc.Dial(member.Addr.String(), grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Could not connect: %s", err)
			}

			// Defer means: When this function returns, call this method (meaing, one main is done, close connection)
			defer conn.Close()

			//  Create new Client from generated gRPC code from proto
			c := pb.NewDMEClient(conn)
			message := pb.AccesRequest{Timestamp: &pb.Timestamp{Events: Time.time}, RequestingId: int32(this.id)}
			response, err := c.RequestAccess(context.Background(), &message)

			if err != nil {
				log.Fatalf("Error when calling RequestAccess: %s", err)
			}
			if response.ResponseGranted {
				noOfResponse++
			}

		}
	}
	if noOfResponse == len(otherMembers) {
		state = "HELD"
	}
}

func (node *Node) RequestAccess(ctx context.Context, in *pb.AccesRequest) (*pb.AccessResponse, error) {
	if state == "HELD" || (state == "WANTED" && (Time.time < in.Timestamp.Events)) {
		queue = append(queue, Node{id: int(in.RequestingId)})
	}
	return nil, nil
}

func listen() {
	lis, err := net.Listen("tcp", this.port)
	if err != nil {
		log.Fatalf("failed to listen on port: "+this.port, err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterDMEServer(grpcServer, &this)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve on port: "+this.port, err)
	}
}

func connectToNetwork() {
	for i := 2; i < len(os.Args); i++ {
		var node Node
		node.port = os.Args[i]
		node.id = i - 2
		network = append(network, node)
	}
	this.port = network[this.id].port
}
>>>>>>> Stashed changes

func SetUpLog() {
	var filename = "log " + os.Args[0]
	LOG_FILE := filename
	logFile, err := os.OpenFile(LOG_FILE, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logFile)
}

func setupCluster(advertiseAddr string, clusterAddr string) (*serf.Serf, error) {
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.AdvertiseAddr = advertiseAddr

	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create cluster")
	}

	_, err = cluster.Join([]string{clusterAddr}, true)
	if err != nil {
		log.Printf("Couldn't join cluster, starting own: %v\n", err)
	}

	return cluster, nil
}

func getOtherMembers(cluster *serf.Serf) []serf.Member {
	members := cluster.Members()
	for i := 0; i < len(members); {
		if members[i].Name == cluster.LocalMember().Name || members[i].Status != serf.StatusAlive {
			if i < len(members)-1 {
				members = append(members[:i], members[i+1:]...)
			} else {
				members = members[:i]
			}
		} else {
			i++
		}
	}
	return members
}
