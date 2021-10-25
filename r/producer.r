library(rkafka)

producer1 <- rkafka.createProducer("127.0.0.1:9092")

while (TRUE) {
    cat("message: ")

    message <- readLines("stdin", n = 1)

    if (!message == "") {
        rkafka.send(producer1, "r_topic", "127.0.0.1:9092", message)
    } else {
        break
    }
}

rkafka.closeProducer(producer1)