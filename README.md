# M3 Distributed Key-Value Store Project

**CSC 6712 - Project 3:  Distributed Key-Value Store**

Grading Option: **AB-Level**

---

## Overview

This project uses the client-server concept from the last lab and expands on it to store key-value pairs on multiple
servers. Through a distributed client, the key along with the IP address and port number of the servers are used to 
determine where the key-value pair should go by putting all components into a `Murmur hash` and taking the $S$ servers
where $S = (N / 2) + 1$. This $S$ servers will be used to store and confirm that a command was successfully received.
For `TRANSACT` commands, this is broken down per server to accommodate that some servers are only supposed to have a
subset of the keys. 


## Requirements
- Java 17

### Starting up the Project via Source Code
To get started with the source code, make sure to download the version of Java needed for the project and download
the code. Using the Linux command line, use `chmod +x gradlew` then `./gradlew run` to boot up the project
**OR** `./gradlew test`.
- For **Windows**, it'll just be `gradlew.bat run` **OR** `gradlew.bat test`

This should kickstart the dependency download and build process.

#### Argument Flags
Conducting `./gradlew run [OPTIONAL-PORT]` without any flags will start the code as a server. The user can specify a
port to use for the server to communicate on.

Conducting `./gradlew run -c [OPTIONAL IP:PORT]` OR `./gradlew run --client [OPTIONAL IP:PORT]` will run the code as a client.
A second argument can be provided to specify the PORT number that the server lives on, but will assume `127.0.0.1:8080`
as the default connection.

Conducting `./gradlew run -mc` OR `./gradlew run --multi-client` will run the code as a client that connects to
multiple servers. The servers are specified in the `config.yml` under the `self` list. Changing the quantity or the
port numbers will reflect in the client as well when multi-client mode is run again.

Conducting `./gradlew run -ms` OR `./gradlew run --multi-server` will run the code as a group of servers that utilize 
their own B-Trees and tree-log files for reconstruction. Like multi-clint mode, it uses the `self` tab to create the 
server instances from the `config.yml` so changes to the quantity or the port numbers will reflect in the servers
when multi-server mode is run again.
