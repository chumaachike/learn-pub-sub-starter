package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}
	defer channel.Close()

	fmt.Println("Peril game client connected to RabbitMQ!")

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gamestate := gamelogic.NewGameState(name)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, "pause."+name, routing.PauseKey, pubsub.QueueTransient, handlerPause(gamestate))
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+name, routing.ArmyMovesPrefix+".*", pubsub.QueueTransient, handlerMove(gamestate, channel))
	if err != nil {
		log.Fatalf("could not subscribe to army moves: %v", err)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.QueueDurable,
		handlerWar(gamestate, channel),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
	}
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "move":
			mv, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			routingKey := fmt.Sprintf(
				"%s.%s",
				routing.ArmyMovesPrefix,
				name,
			)

			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				routingKey,
				mv,
			)
			if err != nil {
				fmt.Printf("could not publish move: %v\n", err)
				continue
			}

			log.Println("move published")

		case "spawn":
			if err := gamestate.CommandSpawn(words); err != nil {
				fmt.Println(err)
				continue
			}

		case "status":
			gamestate.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			if len(words) < 2 {
				fmt.Println("invalid command..")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("cannot convert to number: %v\n", err)
				continue
			}
			for range n {
				maliciousLog := gamelogic.GetMaliciousLog()
				pubsub.PublishJSON(channel, routing.ExchangePerilTopic, routing.GameLogSlug+"."+name, maliciousLog)
			}

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("unknown command")
		}
	}

}
