package ipec

import "github.com/DrakeW/go-ipec/pb"

type Task struct {
	*pb.Task
}

type TaskRequest struct {
	*pb.TaskRequest
}

type TaskResponse struct {
	*pb.TaskResponse
}

type TaskOwner interface {
	CreateTask(function, input []byte, description string) Task
	CreateTaskRequest(*Task) *TaskRequest
	Dispatch(*TaskRequest) (*TaskResponse, error)
	HandleTaskResponse(*TaskResponse) error
}

type TaskPerformer interface {
	HandleTaskRequest(*TaskRequest) (*TaskResponse, error)
	SendTaskResponse(*TaskResponse) error
}

var protocolID = "/ipec/task/0.1.0"
