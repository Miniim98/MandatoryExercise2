when starting a node in the system on should pass the following arguments:

int id: the id of the node should be between 0 and the total number of nodes
list of ports: the ports that this and other nodes will be using. 
the number of ports should be equal to the amount of nodes

an example:

go run . 2 8008 8080 0808