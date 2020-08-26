# Inter-Planetary Elastic Computation

IPEC (Inter-Planetary Elastic Computation) is a decentralized network of nodes collectively take on computation tasks in a distributed manner. Nodes can join the network as a task creator, performer or both. The goal of this network is to harness the under-utilized computation power of private servers, public cloud and even personal devices to take on additional computation tasks in an autonomous fashion when they are not under heavy load.

For now, the tasks dispatched through the network must be "mappable" to task performer nodes and "reducible" on the task dispatcher node.

## Proof-of-Concept

This project is a POC of the IPEC network, its scope is much smaller than the grand goal. Scope of the POC:

- ~~Basic communication protocols between task owner and performer~~
- ~~Task: contains an executable and input~~
- Task Performer
  - ~~will be chosen on a first respond first serve basis~~
  - ~~will always choose to execute the task (with random sleep and store the task on disk)~~
- The daemon process supports a gRPC API
- Task Submission will be done through a CLI application that communicates over the gRPC API

## Design

The network contains several modules:

- communication (protocols)
- storage (for task, input, output storage)
- scheduler (figure out when to run a task, depending on the node's load and task urgency)
- metering (measures "computation" for each task execution)
- reward (TBD)
