# Scalability Engineering Prototype



## How to run

Some examples. Default startup is 5 nodes and 3 load-balancers.

```sh
docker-compose up --build
```

```sh
docker-compose up --scale node=5
```

```sh
ddocker-compose up --scale node=2 -d --scale load-balancer=1
```

## How to test
Start the scripts under /test or use a request client like Postman.