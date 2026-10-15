
# Day 24 - Stateless vs Stateful Streaming

## Objective
Learn stateless and stateful processing using Scala and Spark Streaming.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.9
- Spark Streaming DStreams
- sbt

## Scenario
Track running transaction counts for each bank account.

## Stateless Processing
Uses `reduceByKey` to count transactions in the current batch only.

## Stateful Processing
Uses `updateStateByKey` to accumulate counts across batches.

## Run the Project

Compile:

```bash
sbt compile
```

Terminal 1:

```bash
nc -lk 9999
```

Terminal 2:

```bash
sbt "runMain Day24StatelessStateful"
```

Enter one bank account ID per line in Terminal 1.

## Key Learning
Stateless processing works on the current batch; stateful processing
maintains accumulated counts across batches.
