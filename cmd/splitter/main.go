package main

import (
	"github.com/gin-gonic/gin"
	"github.com/go-markitdown/internal/handler"
)

func main() {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	v1 := router.Group("/api/v1")
	v1.POST("/split-file", handler.SplitFile)

	router.Run() // listens on 0.0.0.0:8080 by default
}
