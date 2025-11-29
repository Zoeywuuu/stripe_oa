"""
Load balance Problem-
You are implementing a load balancer for a notebook platform that runs multiple Jupyter servers. 
The goal is to decide which server (target) should handle each incoming websocket connection request, 
balancing load and applying various constraints. 

The function to implement is route_requests(numTargets, maxConnectionsPerTarget, requests), 
which processes a series of requests and returns a log of all successful connections. 
The parameters are: 
numTargets, the number of servers; 
maxConnectionsPerTarget, the maximum number of active connections allowed per target; 
requests, which is a list of strings describing each request. 

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

# 修正后的完整代码

def route_requests1(numTargets, maxConnectionsPerTarget, requests):
    """
    Part 1: 基础负载均衡
    - 选择负载最小的服务器
    - 平局时选索引最小的
    """
    serverLoad = [0] * (numTargets + 1)  # 1-based索引
    logs = []

    for req in requests:
        parts = req.split(',')
        action, connId, userId, objId = parts
        
        # 找负载最小的服务器，平局选索引小的
        best = 1
        bestLoad = serverLoad[1]
        for t in range(2, numTargets + 1):
            if serverLoad[t] < bestLoad:
                bestLoad = serverLoad[t]
                best = t
        
        serverLoad[best] += 1
        logs.append(f"{connId},{userId},{best}")

    return logs


def route_requests2(numTargets, maxConnectionsPerTarget, requests):
    """
    Part 2: 处理断开连接
    - 处理CONNECT和DISCONNECT
    - DISCONNECT时减少服务器负载
    """
    serverLoad = [0] * (numTargets + 1)
    connToServer = {}  # connectionId -> targetIndex
    logs = []

    for req in requests:
        parts = req.split(',')
        action, connId, userId, objId = parts

        if action == "DISCONNECT":
            if connId in connToServer:
                target = connToServer[connId]
                serverLoad[target] -= 1
                del connToServer[connId]
                
        elif action == "CONNECT":
            # 找负载最小的服务器
            best = 1
            bestLoad = serverLoad[1]
            for t in range(2, numTargets + 1):
                if serverLoad[t] < bestLoad:
                    bestLoad = serverLoad[t]
                    best = t

            # 分配连接
            serverLoad[best] += 1
            connToServer[connId] = best
            logs.append(f"{connId},{userId},{best}")
            
    return logs


def route_requests3(numTargets, maxConnectionsPerTarget, requests):
    """
    Part 3: 基于对象ID的路由
    - 相同objectId必须连接到同一服务器
    - 第一次连接时固定，最后一个断开时释放
    """
    serverLoad = [0] * (numTargets + 1)
    connToServer = {}
    logs = []

    objectPin = {}      # objectId -> 固定的服务器
    objectCount = {}    # objectId -> 活跃连接数

    for req in requests:
        parts = req.split(',')
        action, connId, userId, objId = parts

        if action == "CONNECT":
            # 检查对象是否已固定
            if objId in objectPin:
                target = objectPin[objId]
            else:
                # 按负载最少选择服务器
                best = 1
                bestLoad = serverLoad[1]
                for t in range(2, numTargets + 1):
                    if serverLoad[t] < bestLoad:
                        bestLoad = serverLoad[t]
                        best = t
                target = best
                objectPin[objId] = target  # 固定对象到服务器

            # 分配连接
            serverLoad[target] += 1
            connToServer[connId] = target
            logs.append(f"{connId},{userId},{target}")

            # 更新对象连接计数
            objectCount[objId] = objectCount.get(objId, 0) + 1

        elif action == "DISCONNECT":
            if connId in connToServer:
                target = connToServer[connId]
                serverLoad[target] -= 1
                del connToServer[connId]

                # 更新对象连接计数
                objectCount[objId] -= 1
                if objectCount[objId] == 0:
                    # 释放对象固定
                    del objectCount[objId]
                    del objectPin[objId]

    return logs


def route_requests4(numTargets, maxConnectionsPerTarget, requests):
    """
    Part 4: 最大容量限制
    - 服务器有最大连接数限制
    - 超过容量的连接会被拒绝
    """
    serverLoad = [0] * (numTargets + 1)
    connToServer = {}
    logs = []

    objectPin = {}
    objectCount = {}

    for req in requests:
        parts = req.split(',')
        action, connId, userId, objId = parts

        if action == "CONNECT":
            # 检查对象是否已固定
            if objId in objectPin:
                target = objectPin[objId]
                if serverLoad[target] >= maxConnectionsPerTarget:
                    # 固定的服务器已满 -> 拒绝
                    continue
            else:
                # 找负载最少且未满的服务器
                candidates = []
                minLoad = float("inf")
                for t in range(1, numTargets + 1):
                    if serverLoad[t] < maxConnectionsPerTarget:
                        if serverLoad[t] < minLoad:
                            minLoad = serverLoad[t]
                            candidates = [t]
                        elif serverLoad[t] == minLoad:
                            candidates.append(t)

                if not candidates:
                    # 所有服务器都满 -> 拒绝
                    continue

                target = min(candidates)  # 平局选索引小的
                objectPin[objId] = target

            # 分配连接
            serverLoad[target] += 1
            connToServer[connId] = target
            logs.append(f"{connId},{userId},{target}")

            # 更新对象连接计数
            objectCount[objId] = objectCount.get(objId, 0) + 1

        elif action == "DISCONNECT":
            if connId in connToServer:
                target = connToServer[connId]
                serverLoad[target] -= 1
                del connToServer[connId]

                # 更新对象连接计数
                objectCount[objId] -= 1
                if objectCount[objId] == 0:
                    del objectCount[objId]
                    del objectPin[objId]

    return logs


def route_requests(numTargets, maxConnectionsPerTarget, requests):
    """
    Part 5: 服务器关闭
    - 处理SHUTDOWN命令
    - 关闭服务器上的连接需要重新路由
    - 按connectionId升序重新路由
    """
    serverLoad = [0] * (numTargets + 1)
    connToServer = {}
    logs = []

    objectPin = {}
    objectCount = {}
    connMeta = {}  # 保存连接的元数据

    for req in requests:
        parts = req.split(',')
        action = parts[0]

        if action == "CONNECT":
            _, connId, userId, objId = parts
            connMeta[connId] = (userId, objId)

            # 对象是否已固定？
            if objId in objectPin:
                target = objectPin[objId]
                if serverLoad[target] >= maxConnectionsPerTarget:
                    continue  # 拒绝
            else:
                # 找负载最少且未满的服务器
                candidates = []
                minLoad = float("inf")
                for t in range(1, numTargets + 1):
                    if serverLoad[t] < maxConnectionsPerTarget:
                        if serverLoad[t] < minLoad:
                            minLoad = serverLoad[t]
                            candidates = [t]
                        elif serverLoad[t] == minLoad:
                            candidates.append(t)
                            
                if not candidates:
                    continue  # 拒绝
                    
                target = min(candidates)
                objectPin[objId] = target

            serverLoad[target] += 1
            connToServer[connId] = target
            objectCount[objId] = objectCount.get(objId, 0) + 1
            logs.append(f"{connId},{userId},{target}")

        elif action == "DISCONNECT":
            _, connId, userId, objId = parts
            if connId in connToServer:
                target = connToServer[connId]
                serverLoad[target] -= 1
                del connToServer[connId]

                objectCount[objId] -= 1
                if objectCount[objId] == 0:
                    del objectCount[objId]
                    del objectPin[objId]

        elif action == "SHUTDOWN":
            _, shutdownTarget = parts
            shutdownTarget = int(shutdownTarget)

            # 找出所有需要被驱逐的连接
            evicted = [cid for cid, t in connToServer.items() if t == shutdownTarget]
            evicted.sort()  # 按connectionId升序重新路由

            # 从服务器移除并更新对象计数
            for cid in evicted:
                userId, objId = connMeta[cid]
                serverLoad[shutdownTarget] -= 1
                del connToServer[cid]

                objectCount[objId] -= 1
                if objectCount[objId] == 0:
                    del objectCount[objId]
                    del objectPin[objId]

            # 重新路由每个被驱逐的连接
            for cid in evicted:
                userId, objId = connMeta[cid]

                # 对象是否已固定？
                if objId in objectPin:
                    target = objectPin[objId]
                    if serverLoad[target] >= maxConnectionsPerTarget:
                        continue  # 拒绝
                else:
                    # 找负载最少且未满的服务器
                    candidates = []
                    minLoad = float("inf")
                    for t in range(1, numTargets + 1):
                        if t == shutdownTarget:
                            continue  # 不能路由回关闭的服务器
                        if serverLoad[t] < maxConnectionsPerTarget:
                            if serverLoad[t] < minLoad:
                                minLoad = serverLoad[t]
                                candidates = [t]
                            elif serverLoad[t] == minLoad:
                                candidates.append(t)

                    if not candidates:
                        continue  # 拒绝
                        
                    target = min(candidates)
                    objectPin[objId] = target

                # 分配重新路由的连接
                serverLoad[target] += 1
                connToServer[cid] = target
                objectCount[objId] = objectCount.get(objId, 0) + 1
                logs.append(f"{cid},{userId},{target}")

    return logs