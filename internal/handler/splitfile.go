package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/go-markitdown/internal/usecase"
)

type splitFileRequest struct {
	RequestID      string `json:"request_id" binding:"required"`
	ConversationID string `json:"conversation_id" binding:"required"`
	MessageID      string `json:"message_id" binding:"required"`
	FilePath       string `json:"file_path" binding:"required"`
}

type splitFileResponse struct {
	URLs []string `json:"urls"`
}

// SplitFile handles POST /api/v1/split-file.
func SplitFile(c *gin.Context) {
	var req splitFileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	requestID, err := uuid.Parse(req.RequestID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request_id: " + err.Error()})
		return
	}
	conversationID, err := uuid.Parse(req.ConversationID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid conversation_id: " + err.Error()})
		return
	}
	messageID, err := uuid.Parse(req.MessageID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message_id: " + err.Error()})
		return
	}

	urls, err := usecase.SplitFile(c.Request.Context(), usecase.SplitFileInput{
		RequestID:      requestID,
		ConversationID: conversationID,
		MessageID:      messageID,
		FilePath:       req.FilePath,
	})
	if err != nil {
		if errors.Is(err, usecase.ErrUnsupportedFormat) || errors.Is(err, usecase.ErrOLENotSplittable) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, splitFileResponse{URLs: urls})
}
