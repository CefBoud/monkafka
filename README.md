# MonKafka 
MonKafka is a minimal, zero-dependency implementation of a basic Kafka broker.

In its current state, the code handles topic creation, produce and consume requests. The mechanics are still very basic and have a long way to go.

I am also writing a [blog post](https://cefboud.github.io/posts/monkafka) to share some of my learnings along the way.

# Testing
I am running `go1.23.4` for my local testing.

1. Start the server locally on port 9092:

    ```go
    go run main.go

    Server is listening on port 9092...
    ```
2. I am testing with Kafka 3.9:

    
    ```bash
    # Download Kafka
    cd /path/to/workdir
    curl -O https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz 
    tar -xzf kafka_2.13-3.9.0.tgz
    cd kafka_2.13-3.9.0

    # Create topic
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

3. a simple performance test:

    Producer:

    ```
    kafka_2.13-3.9.0> ./bin/kafka-producer-perf-test.sh \
    --topic perf1 \
    --num-records 1000000 \
    --record-size 500 \
    --throughput 100000 \
    --producer-props acks=1 batch.size=16384 linger.ms=5 bootstrap.servers=localhost:9092
    499713 records sent, 99942.6 records/sec (47.66 MB/sec), 3.9 ms avg latency, 113.0 ms max latency.
    1000000 records sent, 99820.323418 records/sec (47.60 MB/sec), 2.58 ms avg latency, 113.00 ms max latency, 1 ms 50th, 14 ms 95th, 53 ms 99th, 70 ms 99.9th.
    ```
    On another terminal, the consumer:
    ```
    kafka_2.13-3.9.0> ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic perf1

    VZQSSGFHLARNQBGJMONQNTUKZOSTUGMIWDTUFOFRGKGHMTAYZDRVCNEYVWPMCVUUXKDOZDZEICLPMDBKWJYHISCPFKLSHDYMYIZHKAHUJUE
    LGMYCJRGBQACNCXENKWHQUPXFEPZWVIRGBEHOSSZKEHATFNUQGQDMIPOKENUKVDBYQAVRTCOEFTTYTFZMMBHCUHYQKLDFEBVCALNZVMBMFUTYFWHPAEIYVLYDJCQRHCOMOOVMYMDRVSASUNUSDQKPBZLUMOJQFVOHTKDJXALHHZEVZZGYWEDDTYDKONOQUNYYNQV
    .
    .
    .

    ^C Processed a total of 1000000 messages

    ```


# TODO
- [X] Basic Topic Creation, Produce and Fetch.
- [ ] Multiple log segments per partition
- [ ] Parse requests properly (so far, only necessary fields are parsed)
- [ ] Configurability 
- [ ] Consumer offsets topic
- [ ] Better concurrency
- [ ] Error handling (e.g. topic not found)
- [ ] Honor more produce and consume options
- [ ] Distributed system / multiple brokers


# Credits
[Jocko](https://github.com/travisjeffery/jocko) has been a major inspiration for this work.