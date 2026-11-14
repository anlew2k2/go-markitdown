package main

import (
	"github.com/anthony-lewis/go-markitdown/internal/handler"
	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	v1 := router.Group("/api/v1")
	v1.POST("/subprocess/run", handler.RunSubProcess)
	v1.POST("/subprocess/markitdown/run", handler.RunMarkitdown)

	router.Run() // listens on 0.0.0.0:8080 by default
}
