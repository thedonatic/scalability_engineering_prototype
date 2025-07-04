# Scalability Engineering Prototype



## How to run

```sh
docker-compose up --build
```

```sh
docker-compose run --rm client python client.py --client-id demo1
```

```sh
docker-compose up -d --scale node=3
```

1. State management:
The application manages state using sqlite for persisting key-value pairs.
2. Scalability:
The architecture supports horizontal scaling by allowing multiple nodes to join, register, and participate in quorum-based operations. The load balancer distributes requests across available nodes and is stateless and can be scaled easily.
3. Overload mitigation:
Every component is designed to handle overload gracefully.

Timeouts, retries, and backoff with jitter in the client
Workload isolation using shuffle-sharding in the load balancer
Load shedding in the node