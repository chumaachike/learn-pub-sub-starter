package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	dat, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ch.PublishWithContext(context.Background(),
		exchange, key, false, false,
		amqp.Publishing{ContentType: "application/json", Body: dat})
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	isDurable := queueType == QueueDurable

	q, err := ch.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, nil)
	if err = ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, q, nil
}
