package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
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

func createTaskHandler(c *gin.Context, db *gorm.DB) {
	var task Task
	if err := c.ShouldBindJSON(&task); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	task.Status = "pending"
	if err := db.Create(&task).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "task created"})
}

func main() {
	dsn := "user=postgres password=postgres dbname=tasks port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect to database")
	}

	db.AutoMigrate(&Task{})

	r := gin.Default()
	r.POST("/tasks", func(c *gin.Context) {
		createTaskHandler(c, db)
	})

	r.Run(":8080")
}
