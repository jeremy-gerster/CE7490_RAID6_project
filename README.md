# Student project for course **CE7490: ADVANCED TOPICS IN DISTRIBUTED SYSTEMS**

## Preface

**Authors**:  
Jeremy Gerster  
Simon Menke

We thank Prof. Anwitaman Datta for teaching the course and providing insightful lectures.

## Description

This project is an implementation of RAID6, a data storage technology that provides fault tolerance by distributing data and parity blocks across multiple drives. The code simulates the core functionalities of RAID6, including data striping, dual parity calculation, and recovery from up to two simultaneous disk failures. A detailed description about the implementation and our experiments can be found in the report under `report/report.pdf`.

## Instructions

Run `main.py` to interact with our RAID6 implementation.  
**Note:** The remote storage option in the cloud might not be accessible anymore as the servers are only available for a limited time.

## Infrastructure

We deployed our project in the cloud to achieve true independent distribution among nodes. The following explains our infrastructure setup.

### Requirements

Each of the VMs has to have Docker engine installed, instructions can be found here:  
https://docs.docker.com/engine/install/

Ports for internal communication between machines have to be allowed in the firewall.

Ingress port for communication between client and server needs to be allowed in the firewall (usually 8000).

### Setup

We used 8 VMs of type 2-medium on Google Compute Engine due to the quota of up to 8 machines on the free plan. One of the 8 VMs acts as manager node in a Docker swarm, the other 7 are worker nodes (see [Docker swarm](https://docs.docker.com/engine/swarm/)). The 7 worker nodes each have a MongoDB service running. 

The manager node runs a FastAPI server that communicates with the MongoDB instances as well as the client. Code for the server can be found in the file `server.py`

### Guide

A docker swarm can be initiated on a chosen manager node using:

```
docker swarm init
```

The above command outputs a command that can be used on the worker nodes to join the swarm. Simply copy-paste into worker terminals.

You can check which nodes joined the swarm:

```
docker node ls
```

To deploy MongoDB services across the worker nodes, you can specify a constraint with the flag `--constraint 'node.hostname == swarm-wrk7`. The hostname is your chosen name for the VM. You can also give the service a name with `--name mongodb-worker7`.

Full example command:

```
docker service create \
--name mongodb-worker7 \
--constraint 'node.hostname == swarm-wrk7' \
--publish published=27007,target=27017,protocol=tcp \
--mount type=volume,source=mongo_data_worker7,target=/data/db \
mongo:latest
```

If everything went well and ports are enabled, you should be able to talk to the MongoDB instances.
You can test it by installing [mongosh](https://www.mongodb.com/docs/mongodb-shell/install/) on the manager node and running:

```
mongosh "mongodb://localhost:<mongodb_port>"
```
