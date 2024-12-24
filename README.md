# MonKafka 
MonKafka is a minimalist, zero-dependency implementation of part of the Kafka protocol.

In its current state, the code handles topic creation, produce and consume requests. The mechanics are still very basic and have a long way to go.

# Testing
I am running `go1.23.4` for my local testing.

1. Start the server locally on port 9092:

    ```go
    go run main.go

    Server is listening on port 9092...
    ```
2. I am testing with Kafka 3.9:

    ```bash
    bin/kafka-topics.sh --create --topic titi  --bootstrap-server localhost:9092    

    Created topic titi.
    ```

    ```
    kafka_2.13-3.9.0> ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic titi
    >hello
    >is it you?
    >Nope!
    ```
    In another terminal:
    ```
    kafka_2.13-3.9.0> ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic titi  --from-beginning 
    hello
    is it you?
    Nope!
    ```


# TODO
- [X] Basic Topic Creation, Produce and Fetch.
- [ ] Parse requests properly (so far, only necessary fields are parsed)
- [ ] Write only to memory and flush to disk asynchronously (speed up)
- [ ] Manage consumer offsets
- [ ] Handle concurrency properly (Mutex)
- [ ] Honor more produce and consume options

# Credits
[Jocko](https://github.com/travisjeffery/jocko) has been a major inspiration for this work.