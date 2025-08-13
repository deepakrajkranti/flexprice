package v1

import (
	"net/http"

	"github.com/flexprice/flexprice/internal/api/dto"
	ierr "github.com/flexprice/flexprice/internal/errors"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/flexprice/flexprice/internal/service"
	"github.com/flexprice/flexprice/internal/types"
	"github.com/gin-gonic/gin"
)

type TaskHandler struct {
	taskService service.TaskService
	logger      *logger.Logger
}

func NewTaskHandler(taskService service.TaskService, logger *logger.Logger) *TaskHandler {
	return &TaskHandler{
		taskService: taskService,
		logger:      logger,
	}
}

// CreateTask godoc
// @Summary Create a new task
// @Description Create a new import/export task
// @Tags Tasks
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param task body dto.CreateTaskRequest true "Task details"
// @Success 201 {object} dto.TaskResponse
// @Failure 400 {object} ierr.ErrorResponse
// @Failure 500 {object} ierr.ErrorResponse
// @Router /tasks [post]
func (h *TaskHandler) CreateTask(c *gin.Context) {
	var req dto.CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("failed to bind request", "error", err)
		c.Error(ierr.WithError(err).
			WithHint("Please check the request payload").
			Mark(ierr.ErrValidation))
		return
	}

	task, err := h.taskService.CreateTask(c.Request.Context(), req)
	if err != nil {
		h.logger.Error("failed to create task", "error", err)
		c.Error(err)
		return
	}

	c.JSON(http.StatusCreated, task)
}

// GetTask godoc
// @Summary Get a task by ID
// @Description Get detailed information about a task
// @Tags Tasks
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param id path string true "Task ID"
// @Success 200 {object} dto.TaskResponse
// @Failure 404 {object} ierr.ErrorResponse
// @Failure 500 {object} ierr.ErrorResponse
// @Router /tasks/{id} [get]
func (h *TaskHandler) GetTask(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.Error(ierr.WithError(nil).
			WithHint("Please check the task id").
			Mark(ierr.ErrValidation))
		return
	}

	task, err := h.taskService.GetTask(c.Request.Context(), id)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, task)
}

// ListTasks godoc
// @Summary List tasks
// @Description List tasks with optional filtering
// @Tags Tasks
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param filter query types.TaskFilter false "Filter"
// @Success 200 {object} dto.ListTasksResponse
// @Failure 400 {object} ierr.ErrorResponse
// @Failure 500 {object} ierr.ErrorResponse
// @Router /tasks [get]
func (h *TaskHandler) ListTasks(c *gin.Context) {
	var filter types.TaskFilter
	if err := c.ShouldBindQuery(&filter); err != nil {
		h.logger.Error("failed to bind query parameters", "error", err)
		c.Error(ierr.WithError(err).
			WithHint("Please check the query parameters").
			Mark(ierr.ErrValidation))
		return
	}

	if err := filter.Validate(); err != nil {
		c.Error(err)
		return
	}

	resp, err := h.taskService.ListTasks(c.Request.Context(), &filter)
	if err != nil {
		h.logger.Error("failed to list tasks", "error", err)
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, resp)
}

// UpdateTaskStatus godoc
// @Summary Update task status
// @Description Update the status of a task
// @Tags Tasks
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param id path string true "Task ID"
// @Param status body dto.UpdateTaskStatusRequest true "New status"
// @Success 200 {object} dto.TaskResponse
// @Failure 400 {object} ierr.ErrorResponse
// @Failure 404 {object} ierr.ErrorResponse
// @Failure 500 {object} ierr.ErrorResponse
// @Router /tasks/{id}/status [put]
func (h *TaskHandler) UpdateTaskStatus(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.Error(ierr.WithError(nil).
			WithHint("Please check the task id").
			Mark(ierr.ErrValidation))
		return
	}

	var req dto.UpdateTaskStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("failed to bind request", "error", err)
		c.Error(ierr.WithError(err).
			WithHint("Please check the request payload").
			Mark(ierr.ErrValidation))
		return
	}

	if err := req.Validate(); err != nil {
		c.Error(err)
		return
	}

	if err := h.taskService.UpdateTaskStatus(c.Request.Context(), id, req.TaskStatus); err != nil {
		c.Error(err)
		return
	}

	// Get updated task
	task, err := h.taskService.GetTask(c.Request.Context(), id)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, task)
}

// ProcessTask godoc
// @Summary Process a task
// @Description Start processing a task
// @Tags Tasks
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param id path string true "Task ID"
// @Success 202 {object} gin.H
// @Failure 400 {object} ierr.ErrorResponse
// @Failure 404 {object} ierr.ErrorResponse
// @Failure 500 {object} ierr.ErrorResponse
// @Router /tasks/{id}/process [post]
func (h *TaskHandler) ProcessTask(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.Error(ierr.WithError(nil).
			WithHint("Please check the task id").
			Mark(ierr.ErrValidation))
		return
	}

	// Start processing in a goroutine
	go func() {
		if err := h.taskService.ProcessTask(c.Request.Context(), id); err != nil {
			h.logger.Error("failed to process task", "error", err, "task_id", id)
		}
	}()

	c.JSON(http.StatusAccepted, gin.H{"message": "task processing started"})
}
