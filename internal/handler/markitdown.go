package handler

import (
	"errors"
	"net/http"

	"github.com/anthony-lewis/go-markitdown/internal/usecase"
	"github.com/gin-gonic/gin"
)

type runMarkitdownRequest struct {
	URL string `json:"url" binding:"required"`
}

type runMarkitdownResponse struct {
	Markdown string `json:"markdown"`
}

// RunMarkitdown handles POST /api/v1/subprocess/markitdown/run.
func RunMarkitdown(c *gin.Context) {
	var req runMarkitdownRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	markdown, err := usecase.ConvertURLToMarkdown(req.URL)
	if err != nil {
		if errors.Is(err, usecase.ErrUnsupportedFormat) || errors.Is(err, usecase.ErrFileTooLarge) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, runMarkitdownResponse{Markdown: markdown})
}
