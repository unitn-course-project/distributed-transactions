# Optimistic Concurrency Control for Distributed Transactions

In this project, we simulated distributed transactions by implemented optimistic concurrency control algorithms and using the private workspace mechanism. The simulation is involved by several actors: clients, coordinators, and servers.

## Requirements
The system allows multiple clients to make concurrent transactions consisting of sequences of read and write
operations on items of a distributed data store. The protocol must ensure strict serializability for transactions
that are successfully committed, aborting any transaction that would result in an inconsistency. The decision
on whether to commit or abort is taken at the end of the transaction, based on the version of data items at
each server of the store. This approach falls under the “optimistic concurrency control” paradigm: we assume
that conflicts are rare and avoid the overhead of pessimistic locking approaches. Each transaction is handled by
one of the many available coordinators, using a two-phase commit protocol to ensure that all involved servers
agree on the decision.

## Design and Implementaion
### Architecture
- Client (provided): An actor generate transactions.
- Coordinator: An actor handle client request and manipulate transaction between client and server. Furthermore, coordinator also can recovery and tolerate server crash.
- Server: Server is an actor which is in charge of directly operating with data. It also can tolerate server crash and recovery itself after crash.

These actors worked independently and cooperate with each other. The client sends requests which are start transaction, read-write operator, and end transaction, to a random coordinator, and then whenever the transaction is finished, it creates another transaction again, loop until be stopped by the system. The coordinator handles requests from clients and works with the servers in order to serve the request, avoid conflicts, and guarantee the correctness of the system. 

### Implementation
In our project, we implement two different versions. The main idea implementation is the same: associate each transaction with id to manipulate, using lock and private workspace to deal with inconsistent and serialization. The difference is that we implement with varied data structures and protocols (where private workspace is stored (sever or coordinator) and how to run validation phase).


### How to run
- Build `./gradlew build`

- Run v1 `./gradlew :ver1:run`

- Run v2 `./gradlew :ver2:run`

## Authors
- Anh Tu Phan [@anhtu95](https://github.com/anhtu95)
- Tuan Dat Nguyen [@tuandat95cbn](https://github.com/tuandat95cbn)
