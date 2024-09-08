package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

func pickAndSendTasks(db *gorm.DB, rabbitMQURL string, buffer time.Duration) {
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

	q, err := ch.QueueDeclare(
		"task_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Failed to declare a queue:", err)
	}

	for {
		var tasks []Task
		now := time.Now().UTC().Add(buffer)

		err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.
				Where("due_time <= ? AND status = ?", now, "pending").
				Limit(10).
				Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
				Find(&tasks).Error; err != nil {
				return err
			}

			for _, task := range tasks {
				payload, err := json.Marshal(task.Payload)
				if err != nil {
					return err
				}

				err = ch.Publish(
					"",
					q.Name,
					false,
					false,
					amqp.Publishing{
						ContentType: "application/json",
						Body:        payload,
					},
				)
				if err != nil {
					log.Println("Failed to publish task:", err)
					continue
				}

				if err := tx.Model(&task).Update("status", "picked").Error; err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Println("Failed to pick and send tasks:", err)
		}

		time.Sleep(1 * time.Minute) // Poll interval
	}
}
