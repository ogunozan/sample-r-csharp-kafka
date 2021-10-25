library(rkafka)

consumer1 <- rkafka.createConsumer("127.0.0.1:2181",
    "r_topic",
    "r_group",
    consumerTimeoutMs = "60000",
    autoOffsetReset = "smallest"
)

while (TRUE) {
    message <- rkafka.read(consumer1)

    if (message == "q") {
        break
    }

    if (!message == "") {
        print(message)
    }
}

rkafka.closeConsumer(consumer1)