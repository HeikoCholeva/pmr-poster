package main

import(
	"log"
	"sync"
	"time"
	"strings"
	"strconv"
	"net/url"
	"net/http"
	"github.com/Shopify/sarama"
)

func newConsumer() sarama.Consumer {
	log.Println("Creating new Consumer...")
	config := sarama.NewConfig()

	if cfg.SASL.Username != "" && cfg.SASL.Password != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.SASL.Username
		config.Net.SASL.Password = cfg.SASL.Password
	}

	broker := strings.Split(strings.Replace(cfg.Broker, " ", "", -1), ",")
	log.Printf("Broker: %v\n", broker)
	log.Printf("Middleware: %v\n", cfg.Middleware)

	consumer, err := sarama.NewConsumer(broker, config)
	if err != nil {
		log.Fatalf("Consumer error @sarama.NewConsumer: %v\n", err)
	}

	log.Println("Created new Consumer")
	return consumer
}

func newPartitionConsumer() sarama.PartitionConsumer {
	log.Println("Creating new PartitionConsumer...")
	part, err := strconv.Atoi(cfg.Partition)
	if err != nil {
		log.Fatalf("strconv.Atoi(%v) error: %v\n", cfg.Partition, err)
	}

	partitionConsumer, err := consumer.ConsumePartition(cfg.Topic, int32(part), sarama.OffsetNewest)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Created new PartitionConsumer")
	return partitionConsumer
}

func startConsumer() {
	log.Println("PartitionConsumer starting...")
	var wg sync.WaitGroup

	tr := &http.Transport{
		DisableCompression: true,
		MaxIdleConnsPerHost: 1,
		MaxIdleConns: 10,
		IdleConnTimeout: 1* time.Second,
	}
	client := &http.Client{Transport: tr}

	log.Println("Ready to accept some incoming messages")
	for {
		wg.Add(1)
		select {
		case msg := <-partitionConsumer.Messages():
			data := url.Values{}
			data.Set("data", string(msg.Value))
			resp, err := client.Post(cfg.Middleware, "application/json", strings.NewReader(string(msg.Value)))
			if err != nil {
				log.Fatalf("HTTP client error @client.Post: %v\n", err)
			}
			log.Printf("HTTP RESULT -> %v - %v", resp.StatusCode, resp.Status)
			defer resp.Body.Close()
			wg.Done()
		}
		wg.Wait()
	}
}
