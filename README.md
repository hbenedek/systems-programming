# Programming Assignment for Systems for Data Science course

### Task 1

Given a directed Graph G = (V , E), use a map-reduce chain to compute the set of vertices that are
reachable in exactly 4 hops from each vertex.

### Task 2

In this task, you will implement the algorithms for storing and retrieving key-value pair on a pool of
peers connected via an overlay network. Each participating node is identified by an id, has a limited
memory (number of keys,value pairs the node can store), knows the ids of its neighbours, and has access
to the router. Any node can however ask other nodes for the ids of their current neighbours at any time.
A node can talk to other nodes by sending messages to other nodes through the router by calling the
router.sendMessage function. There is no other way to contact other nodes. Moreover, you cannot
store more than the memory supports.


