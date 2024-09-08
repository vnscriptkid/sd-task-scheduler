package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
	"gorm.io/gorm"
)

type Task struct {
	ID        uint      `gorm:"primaryKey"`
	Name      string    `gorm:"not null"`
	DueTime   time.Time `gorm:"not null"`
	Status    string    `gorm:"not null;default:pending"`
	Payload   map[string]interface{}
	CreatedAt time.Time `gorm:"autoCreateTime"`
	UpdatedAt time.Time `gorm:"autoUpdateTime"`
}

func executeTasks(db *gorm.DB, rabbitMQURL string) {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to open a channel:", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		"task_queue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Failed to register a consumer:", err)
	}

	for msg := range msgs {
		var task Task
		// Deserialize task from msg.Body
		if err := json.Unmarshal(msg.Body, &task); err != nil {
			log.Println("Failed to unmarshal task:", err)
			continue
		}
		// Execute task logic here
		log.Printf("Executing task: %s", task.Name)

		// Mark task as completed
		if err := db.Transaction(func(tx *gorm.DB) error {
			return tx.Model(&task).Update("status", "completed").Error
		}); err != nil {
			log.Println("Failed to mark task as completed:", err)
		}
	}
}
