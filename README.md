# CSC568_Project

## Distributed Key Value Store

### Execution:
* Set up zookeeper in clustered mode on a set of nodes
* Run the Mserver.py on a node designated to be the Masternode (it takes its own private/public IP as the argument)
* Run the server.py on the slave nodes in the cluster (for stability join one every 4 seconds, takes its own IP and Masternode's IP as argument)
* Run ClientScript.py on any machine which can reach the Masternode (takes its own IP and either the slave or Masternode's IP as argument)

### Functionality Supported:
* Handles 1 slave node failure every 40 seconds (can be changed)
* Distributes keys evenly among the Masternode and slavenode
* Supports GET (retrieve), PUT (store) and DEL (delete) on key value pairs

Due to the functionality supported, it cannot handle sudden DEL and PUT on same key, as it results in inconsistency
It takes each node 2x40 secs to completely forget a value and its associated key 






