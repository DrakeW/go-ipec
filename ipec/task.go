package ipec

import (
	"context"
	"io/ioutil"

	"github.com/DrakeW/go-ipec/pb"
	"github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	log "github.com/sirupsen/logrus"
)

// TaskOwner - A libp2p host that can create and dispatch task to TaskPerformer
type TaskOwner interface {
	host.Host
	CreateTask(function, input []byte, description string) *pb.Task
	CreateTaskRequest(*pb.Task) *pb.TaskRequest
	Dispatch(*pb.TaskRequest) (*pb.TaskResponse, error)
	HandleTaskResponse(*pb.TaskResponse) error
}

// TaskPerformer - A libp2p host that performs task
type TaskPerformer interface {
	host.Host
	HandleTaskRequest(*pb.TaskRequest) (*pb.TaskResponse, error)
}

// TaskOwnerPerformer - a libp2p host that can both create and perform tasks
type TaskOwnerPerformer interface {
	TaskOwner
	TaskPerformer
}

var taskRequestProtocolID protocol.ID = "/ipec/task/request/0.1.0"
var taskResponseProtocolID protocol.ID = "/ipec/task/response/0.1.0"

// TaskService - handles the communication between TaskOwner and TaskPerformers
type TaskService struct {
	p             TaskOwnerPerformer
	cTaskResult   map[string]chan *pb.TaskResponse
	cNewTaskOwner chan string
}

// NewTaskService - create new task service and register its stream handlers
func NewTaskService(ctx context.Context, p TaskOwnerPerformer) *TaskService {
	ts := &TaskService{
		p:             p,
		cTaskResult:   make(map[string]chan *pb.TaskResponse),
		cNewTaskOwner: make(chan string),
	}
	ts.p.SetStreamHandler(taskRequestProtocolID, ts.handleTaskRequest)
	ts.p.SetStreamHandler(taskResponseProtocolID, ts.handleTaskResponse)

	go ts.loop(ctx)

	return ts
}

func (ts *TaskService) loop(ctx context.Context) {
	for {
		select {
		case newPeer := <-ts.cNewTaskOwner:
			go ts.peerLoop(ctx, newPeer)

		case <-ctx.Done():
			log.Info("Exiting task service background loop...")
			break
		}
	}
}

func (ts *TaskService) peerLoop(ctx context.Context, peerID string) {
	for {
		select {
		case resp := <-ts.cTaskResult[peerID]:
			if err := ts.sendTaskResponse(peerID, resp); err != nil {
				log.Errorf("Failed to send task response to peer %s - Error: %s", peerID, err.Error())
			}
			close(ts.cTaskResult[peerID])

		case <-ctx.Done():
			log.Infof("Exiting background loop for peer %s", peerID)
			break
		}
	}
}

func (ts *TaskService) handleTaskRequest(s network.Stream) {
	taskReq, err := readTaskRequest(s)
	if err != nil {
		s.Reset()
		return
	}
	log.Infof("Received task request from perr %s - Task ID: %s", taskReq.Owner.HostId, taskReq.Task.TaskId)

	if err = writeTaskReqAck(s, taskReq.Task.TaskId); err != nil {
		s.Reset()
		return
	}

	if _, ok := ts.cTaskResult[taskReq.Owner.HostId]; !ok {
		ts.cTaskResult[taskReq.Owner.HostId] = make(chan *pb.TaskResponse, 1)
		ts.cNewTaskOwner <- taskReq.Owner.HostId
	}

	go func() {
		resp, err := ts.p.HandleTaskRequest(taskReq)
		if err != nil {
			resp = &pb.TaskResponse{
				Status: pb.TaskResponse_FAILED,
				TaskId: taskReq.Task.TaskId,
				Output: []byte(err.Error()),
			}
		}
		ts.cTaskResult[taskReq.Owner.HostId] <- resp
	}()
}

// TODO: implement this
func (ts *TaskService) handleTaskResponse(s network.Stream) {}

func (ts *TaskService) sendTaskResponse(peerID string, resp *pb.TaskResponse) error {
	return nil
}

func readTaskRequest(s network.Stream) (*pb.TaskRequest, error) {
	data, err := ioutil.ReadAll(s)
	if err != nil {
		return nil, err
	}
	taskReq := &pb.TaskRequest{}
	if err = proto.Unmarshal(data, taskReq); err != nil {
		return nil, err
	}

	return taskReq, nil
}

func writeTaskReqAck(s network.Stream, taskID string) error {
	taskAck := &pb.TaskResponse{
		TaskId: taskID,
		Status: pb.TaskResponse_ACCEPT,
	}

	data, err := proto.Marshal(taskAck)
	if err != nil {
		return err
	}

	if _, err := s.Write(data); err != nil {
		return err
	}
	return nil
}
