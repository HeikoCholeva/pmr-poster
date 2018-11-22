package main

import (
	"io"
	"os"
	"log"
	"github.com/Shopify/sarama"
)

var (
	cfg Config
	consumer sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
)

func main() {

	log.Println("Initializing log \"client.log\"")
	f, err := os.OpenFile("client.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	mw := io.MultiWriter(os.Stdout, f)
	log.SetOutput(mw)

	log.Println("Initializing properties \"client.properties\"")
	err = cfg.FromFile("client.properties")
	if err != nil {
		panic(err)
	}

	log.Println("Initializing new Consumer")
	consumer = newConsumer()
	log.Println("Initializing new PartitionConsumer")
	partitionConsumer = newPartitionConsumer()
	log.Println("Initializin PartitionConsumer")
	startConsumer()
}
