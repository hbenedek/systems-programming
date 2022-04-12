// YOUR_FULL_NAME_HERE
package task2

import scala.collection.mutable.Queue
import scala.collection.mutable.Set
import scala.util.Random

class MyNode(id: String, memory: Int, neighbours: Vector[String], router: Router) extends Node(id, memory, neighbours, router) {
    val STORE = "STORE"
    val STORE_SUCCESS = "STORE_SUCCESS"
    val STORE_FAILURE = "STORE_FAILURE"
    val RETRIEVE = "RETRIEVE"
    val GET_NEIGHBOURS = "GET_NEIGHBOURS"
    val NEIGHBOURS_RESPONSE = "NEIGHBOURS_RESPONSE"
    val SIMPLE_RETRIEVE  = "SIMPLE_RETRIEVE"
    val RETRIEVE_SUCCESS = "RETRIEVE_SUCCESS"
    val RETRIEVE_FAILURE = "RETRIEVE_FAILURE"
    val INTERNAL_ERROR = "INTERNAL_ERROR"
    val REPLICATE = "REPLICATE"
    val REPLICA_STORE_SUCCESS = "REPLICA_STORE_SUCCESS"
    val USER = "USER"
    val nodes_may_fail = 4

    // helper function for RETRIEVE, returns all nodes in the graph
    def getAllNodes(): List[String] ={
        var explored: List[String] = List()
        var visited: Queue[String] = Queue(this.id)
        while (!visited.isEmpty){
            var v = visited.dequeue()
            router.sendMessage(this.id, v, new Message(this.id, GET_NEIGHBOURS, ""))
                    .data.split(" ")
                    .filter(n => !explored.contains(n))
                    .map(n => visited.enqueue(n))
            explored = explored :+ v
        }
        explored.toList
    }


    override def onReceive(from: String, message: Message): Message = {
        /* 
         * Called when the node receives a message from some where
         * Feel free to add more methods and code for processing more kinds of messages
         * NOTE: Remember that HOST must still comply with the specifications of USER messages
         *
         * Parameters
         * ----------
         * from: id of node from where the message arrived
         * message: the message
         *           (Process this message and decide what to do)
         *           (All communication between peers happens via these messages)
         */
        if (message.messageType == SIMPLE_RETRIEVE) { //simplified version of RETRIEVE, it just checks the node for which the request was send
            val key = message.data 
            val value = getKey(key)
            value match {
                case Some(i) => new Message(id, RETRIEVE_SUCCESS, i)
                case None => new Message(id, RETRIEVE_FAILURE)
            } 
        }
        else if (message.messageType == GET_NEIGHBOURS) { // Request to get the list of neighbours
            new Message(id, NEIGHBOURS_RESPONSE, neighbours.mkString(" "))
        }
        else if (message.messageType == RETRIEVE) { // Request to get the value
            val key = message.data // This is the key
            val value = getKey(key) // Check if the key is present on the node
            var response : Message = new Message("", "", "")
            value match {
                case Some(i) => response = new Message(id, RETRIEVE_SUCCESS, i)
                case None => 
                    //send SIMPLE_RETRIEVE message to all nodes if target node did not have the key
                    val nodes = getAllNodes()
                    // gather responses from nodes, if we have a success we respond that value
                    val success = nodes.map(n => router.sendMessage(id, n, new Message(id, SIMPLE_RETRIEVE, key)))
                                        .filter(_.messageType == RETRIEVE_SUCCESS)
                    if (success.size > 0){
                        response = new Message(id, RETRIEVE_SUCCESS, success.head.data)
                    }
                    else{
                        response = new Message(id, RETRIEVE_FAILURE)
                    } 
                    }
            /*
             * TODO: task 2.1
             * Add retrieval algorithm to retrieve from the peers here
             * when the key isn't available on the HOST node.
             * Use router.sendMessage(from, to, message) to send a message to another node
             */         

            response // Return the correct response message
        }
        else if (message.messageType == STORE) { // Request to store key->value
            /*
             * TODO: task 2.2
             * Change the storage algorithm below to store on the peers
             * when there isn't enough space on the HOST node.
             *
             * TODO: task 2.3
             * Change the storage algorithm below to handle nodes crashing.
             */
            val data = message.data.split("->") // data(0) is key, data(1) is value
            val storedOnSelf = setKey(data(0), data(1)) // Store on current node

            if (storedOnSelf) {
                //send REPLICATE request to random neigbour with the number of replica we want to make
                //message format (key1->value1:number) 
                var replica: Message = new Message("", "", "")
                replica = new Message(id, REPLICATE, message.data.concat(":").concat((nodes_may_fail - 1).toString))

                val gossip = Random.shuffle(neighbours).head
                router.sendMessage(id, gossip, replica)

                // communicate successful query
                new Message(id, STORE_SUCCESS)
            } 
            else{
                //if no memory, send STORE request to random neighbour 
                var store : Message = new Message("", "", "")
                store = new Message(id, STORE, message.data)

                val gossip = Random.shuffle(neighbours).head
                router.sendMessage(id, gossip, store)
            }
        }
        else if (message.messageType == REPLICATE) { // Request to replicate key->value 
            val splitted = message.data.split(":")
            val (kv, num_replicas_left) = (splitted(0), splitted(1).toInt)
            val data = kv.split("->")
            val alreadyStored = getKey(data(0))
            val storedOnSelf = setKey(data(0), data(1))
            if (storedOnSelf && (alreadyStored == None)){
                if (num_replicas_left > 0){
                    var replica: Message = new Message("", "", "")
                    replica = new Message(id, REPLICATE, kv.concat(":").concat((num_replicas_left - 1).toString))

                    val gossip = Random.shuffle(neighbours).head
                    router.sendMessage(id, gossip, replica)
                }
                 // communicate successful query
                new Message(id, REPLICA_STORE_SUCCESS)
            }
            else{
                //if no memory, send REPLICATE request to random neighbour 
                var replica : Message = new Message("", "", "")
                replica = new Message(id, REPLICATE, message.data)

                val gossip = Random.shuffle(neighbours).head
                router.sendMessage(id, gossip, replica)
            }
        }
        /*
         * Feel free to add more kinds of messages.
         */
        else
            new Message(id, INTERNAL_ERROR)
    }
}