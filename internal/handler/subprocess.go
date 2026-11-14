package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-markitdown/internal/services"
)

type runSubProcessRequest struct {
	Args string `json:"args" binding:"required"`
}

type runSubProcessResponse struct {
	Output string `json:"output"`
}

// RunSubProcess handles POST /api/v1/process/run.
func RunSubProcess(c *gin.Context) {
	var req runSubProcessRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	output, err := services.RunCommand(req.Args)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, runSubProcessResponse{Output: output})
}
