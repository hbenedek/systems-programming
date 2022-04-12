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
                case Some(i) => new Message(message.source, RETRIEVE_SUCCESS, i)
                case None => new Message(message.source, RETRIEVE_FAILURE)
            } 
        }
        else if (message.messageType == GET_NEIGHBOURS) { // Request to get the list of neighbours
            new Message(id, NEIGHBOURS_RESPONSE, neighbours.mkString(" "))
        }
        else if (message.messageType == RETRIEVE) { // Request to get the value
            /*
            * TODO: task 2.1
            * Add retrieval algorithm to retrieve from the peers here
            * when the key isn't available on the HOST node.
            * Use router.sendMessage(from, to, message) to send a message to another node
            */         
            val key = message.data 
            var response : Message = new Message("", "", "")
            // store nodes that we already queried and don't contain key
            var explored: List[String] = List()
            // store new unprocessed nodes
            var visited: Queue[String] = Queue(id)
            var isDone = false 
            while (!(visited.isEmpty || isDone)){
                // query SIMPLE RETRIEVE from unprocessed node
                var v = visited.dequeue()
                response = router.sendMessage(id, v, new Message(id, SIMPLE_RETRIEVE, key))
                if (response.messageType == RETRIEVE_SUCCESS){
                    // if we found the key we terminate our loop and return the value with SUCCESS response
                    isDone = true
                }
                 else{
                    // if node does not contain key, we query its neighbours and store unprocessed nodes in 'visited' list
                    router.sendMessage(id, v, new Message(id, GET_NEIGHBOURS, ""))
                        .data.split(" ")
                        .filter(n => !explored.contains(n))
                        .map(n => visited.enqueue(n))
                    // update already explored nodes
                    explored = explored :+ v
                }
            }
            //Return the correct response message
            response
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
                    replica = new Message(message.source, REPLICATE, kv.concat(":").concat((num_replicas_left - 1).toString))
                    router.sendMessage(id, Random.shuffle(neighbours).head, replica)
                }
                // communicate successful query
                new Message(id, REPLICA_STORE_SUCCESS)
            }
            else{
                //if no memory, send REPLICATE request to random neighbour 
                router.sendMessage(id, Random.shuffle(neighbours).head, message)
            }
        }
        /*
         * Feel free to add more kinds of messages.
         */
        else
            new Message(id, INTERNAL_ERROR)
    }
}