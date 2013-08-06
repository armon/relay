relay
=====

Golang framework for simple message passing using an AMQP broker

# Example

Here is an example of a new publisher:

    conf := &relay.Config{Addr: "rabbitmq"}
    conn, err := relay.New(conf)
    defer conn.Close()

    pub, err := conn.Publisher("tasks")
    defer pub.Close()
    pub.Publish("this is a test message")


Here is an example of a new consumer:

    conf := &relay.Config{Addr: "rabbitmq"}
    conn, err := relay.New(conf)
    defer conn.Close()

    cons, err := conn.Consumer("tasks")
    defer cons.Close()

    var msg string
    for {
        cons.Consume(&msg)
        fmt.Printf("Got msg: %s\n", msg)
        cons.Ack()
    }

Here is an example of a consumer using prefetching and multi Acks:

    conf := &relay.Config{Addr: "rabbitmq", PrefetchCount: 5, EnableMultiAck: true}
    conn, err := relay.New(conf)
    defer conn.Close()

    cons, err := conn.Consumer("tasks")
    defer cons.Close()

    var msg string
    for {
        // Consume 5 messages
        for i := 0; i < 5; i++ {
            cons.Consume(&msg)
            fmt.Printf("Got msg: %s\n", msg)
        }

        // Acks the last 5 messages
        cons.Ack()
    }

