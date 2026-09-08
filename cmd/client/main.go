package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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
	name, err := gamelogic.ClientWelcome()
	_, q, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+name, routing.PauseKey, pubsub.QueueTransient)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(q.Name)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	<-ctx.Done()
	fmt.Println("Starting Peril client...")
}
