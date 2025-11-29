"""
Load balance Problem-
You are implementing a load balancer for a notebook platform that runs multiple Jupyter servers. 
The goal is to decide which server (target) should handle each incoming websocket connection request, 
balancing load and applying various constraints. 
The function to implement is route_requests(numTargets, maxConnectionsPerTarget, requests), 
which processes a series of requests and returns a log of all successful connections. 
The parameters are: numTargets, the number of servers; maxConnectionsPerTarget, 
the maximum number of active connections allowed per target; and requests, 
which is a list of strings describing each request. 
Requests can take the form CONNECT,connectionId,userId,objectId; DISCONNECT,connectionId,userId,objectId; or SHUTDOWN,targetIndex. 
The output is a list of strings, each representing a successful CONNECT in the format connectionId,userId,targetIndex, 
where targetIndex is 1-based. Rejected connections do not appear in the output.


The actions are as follows. 
CONNECT means open a new websocket connection. 
DISCONNECT closes a previously established connection. 
SHUTDOWN temporarily shuts down a server, evicts all active connections on it, and reroutes them.

The problem is divided into parts. 
Part 1, basic load balancing: assign new connections to the target with the fewest active connections; 
if there is a tie, pick the smaller index.
For example, with two targets and two connect requests, conn1,userA,obj1 goes to target 1 and conn2,userB,obj2 goes to target 2. 

Part 2, disconnections: when a disconnect request is received, remove that connection, freeing the slot. 
For example, connecting conn1 to target 1, disconnecting it, 
then connecting conn2 would result in both connections logged on target 1. 

Part 3, object pinning: if an object ID is already active, all future connections for that object must go to the same target. 
Once the last connection for that object is disconnected, the pin is removed. 
For example, two connects with the same object ID both go to the same target. 

Part 4, capacity limits: each target cannot exceed maxConnectionsPerTarget. 
If all eligible targets are full, the connection is rejected and not logged. 
For instance, with max connections set to 1 per target, conn1 and conn2 connect to different servers, but conn3 is rejected. 

Part 5, target shutdown: 
a shutdown request on a target evicts all active connections on that target and reroutes them in ascending connection ID order. 
Rerouting follows the same rules as before: fewest active connections, ties broken by smaller index, object pinning still applies, 
and capacity limits are respected. During rerouting, the shutdown target is not considered. 
After rerouting, the target is immediately available again. 
For example, shutting down target 1 causes its connections to be rerouted to target 2, 
then new requests can again be assigned to target 1. 

This problem tests your ability to handle load balancing, disconnection, object pinning, server capacity,
 and rerouting after shutdowns in one system.


"""