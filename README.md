# MonKafka 
A minimalistic implementation of part of the Kafka protocol.

In its current state, the code handles topic creation, produce and consume requests. The mechanics are still very basic and have a long way to go.

# Testing
I am running `go1.23.4` for my local testing.

1. Start the server locally on port 9092:

    ```go
    go run .

    Server is listening on port 9092...
    ```
2. I am testing with Kafka 3.9:

    ```bash
    bin/kafka-topics.sh --create --topic titi  --bootstrap-server localhost:9092    

    Created topic titi.
    ```

    ```
    kafka_2.13-3.9.0 ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic titi
    >hello
    >is it you?
    >Nope!
    ```
    In another terminal:
    ```
    ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic titi  --from-beginning 
    hello
    is it you?
    Nope!
    ```


# TODO
- [X] Simulate Topic Creation
- [ ] Parse requests properly (so far, only necessary fields are parsed)
- [ ] consumer offsets
- [ ] Handle concurrency
- [ ] Honor more produce and consume options

# Credits
[Jocko](https://github.com/travisjeffery/jocko) has been a major inspiration for this work.