# kafka Stream Application

## Introduction

This project aims to implement a real-time stream processing application using the Kafka Streams API.

## Prerequisites

+ OpenJDK 1.8.0

+ Kafka 1.0.0 for Scala 2.11

+ Apache ZooKeeper 3.4.10

ZooKeeper and Kafka servers should be set up in advance.

## Architecture

The application receives two streams (*student topic, classroom topic*) as input, and produces a single stream (*output topic*) as output.

<p align="center"><img src="/README/kafka_streams.png" width="800"></p>

### Inputs

The application consumes messages from two Kafka topics:

+ *student topic* provides information on students and their whereabouts. Each message is a single string of the following form: *Student_ID,Room_ID*.

+ *classroom topic* provides information on classrooms and their capacity. Each message is a single string of the following form: *Room_ID,max_capacity*.

### Outputs

The application outputs two types of messages to the *output topic*:

+ When the current occupancy exceeds the maximum capacity, the application should output the room ID along with some additional information: *Room_ID,max_capacity,current_occupancy*.

+ If the occupancy of a room is decreased subsequently to a value less than or equal the capacity, or if the capacity is increased to a value greater than or equal the occupancy, the application outputs a single string of the following form: *Room_ID,OK*.

If a student enters a room for which there is no room data, then assume that the room’s max capacity is infinite.


## How to run the program


Run the following command to build the application:

```
./build
```

After creating related stream topics (*student topic, classroom topic and output topic*) on Kafka server, you can run the following command to start the stream processing application:

```
java StreamApplication <Kafka_host>:<Kafka_port> <student_topic> <classroom_topic> <output_topic>
```


