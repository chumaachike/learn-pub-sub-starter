package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	channel, err := conn.Channel()
	pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Println("Starting Peril server...")

	<-ctx.Done()

	fmt.Println("Shutting down Peril...")
}
