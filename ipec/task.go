package ipec

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/DrakeW/go-ipec/pb"
	"github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	log "github.com/sirupsen/logrus"
)

// TaskOwner - A libp2p host that can create and dispatch task to TaskPerformer
type TaskOwner interface {
	host.Host
	CreateTask(function, input []byte, description string) *pb.Task
	// Dispatch - dispatch a task to the network and return the task performer peer
	Dispatch(context.Context, *pb.Task) peer.ID
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
	p                  TaskOwnerPerformer
	cTaskResult        map[peer.ID]chan *pb.TaskResponse // a map of task owner to a channel of task result which will be notified when task result is ready
	cNewTaskOwner      chan peer.ID                      // channel to notify this peer starts handling a new task from a specific task owner
	taskToPerformerMap map[string]peer.ID                // keeps track of which performer handles which task
}

// NewTaskService - create new task service and register its stream handlers
func NewTaskService(ctx context.Context, p TaskOwnerPerformer) *TaskService {
	ts := &TaskService{
		p:             p,
		cTaskResult:   make(map[peer.ID]chan *pb.TaskResponse),
		cNewTaskOwner: make(chan peer.ID),
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

func (ts *TaskService) peerLoop(ctx context.Context, peerID peer.ID) {
	for {
		select {
		case resp := <-ts.cTaskResult[peerID]:
			if err := ts.sendTaskResponse(ctx, peer.ID(peerID), resp); err != nil {
				log.Errorf("Failed to send task response to peer %s - Error: %s", peerID, err.Error())
			}
			close(ts.cTaskResult[peerID])

		case <-ctx.Done():
			log.Infof("Exiting background loop for peer %s", peerID)
			break
		}
	}
}

// Dispatch - dispatch a task request to peer
func (ts *TaskService) Dispatch(ctx context.Context, peer peer.ID, req *pb.TaskRequest, chosenC chan bool) error {
	stream, err := ts.p.NewStream(ctx, peer, taskRequestProtocolID)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	if _, err = stream.Write(data); err != nil {
		return err
	}
	// read ACK response from peer
	respData, err := ioutil.ReadAll(stream)
	if err != nil {
		return err
	}

	resp := &pb.TaskResponse{}
	if err = proto.Unmarshal(respData, resp); err != nil {
		return err
	}

	if resp.Status != pb.TaskResponse_ACCEPT {
		return fmt.Errorf("Peer %s did not accept task request", peer)
	}

	defer func() {
		select {
		case chosen := <-chosenC:
			if chosen {
				stream.Write([]byte(req.Task.TaskId))
				ts.taskToPerformerMap[req.Task.TaskId] = peer
			}
			close(chosenC)
		}
	}()

	return nil
}

func (ts *TaskService) handleTaskRequest(s network.Stream) {
	taskReq, err := readTaskRequest(s)
	if err != nil {
		s.Reset()
		return
	}
	log.Infof("Received task request from peer %s - Task ID: %s", taskReq.Owner.HostId, taskReq.Task.TaskId)

	if err = writeTaskReqAck(s, taskReq.Task.TaskId); err != nil {
		s.Reset()
		return
	}

	// read from stream again:
	// - if task ID is returned then this peer is chosen to perform the task
	// - if "na" is returned then this peer is NOT chosen
	chosen, err := ioutil.ReadAll(s)
	if err != nil {
		log.Error("Failed to read ACK response from Task owner")
		s.Reset()
		return
	}
	if string(chosen) == "na" {
		log.Infof("This peer is not chosen as the task performer for task %s", taskReq.Task.TaskId)
		return
	} else if string(chosen) == taskReq.Task.TaskId {
		log.Infof("This peer is chosen as the task performer for task %s", taskReq.Task.TaskId)
	} else {
		log.Warnf("Invalid ACK response received. Abort. - Response: %s", string(chosen))
		return
	}

	taskOwnerID := peer.ID(taskReq.Owner.HostId)
	if _, ok := ts.cTaskResult[taskOwnerID]; !ok {
		ts.cTaskResult[taskOwnerID] = make(chan *pb.TaskResponse, 1)
		ts.cNewTaskOwner <- taskOwnerID
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
		ts.cTaskResult[taskOwnerID] <- resp
	}()
}

// TODO: implement this
func (ts *TaskService) handleTaskResponse(s network.Stream) {
	resp, err := readTaskResponse(s)
	if err != nil {
		s.Reset()
		return
	}

	go ts.p.HandleTaskResponse(resp)
}

func (ts *TaskService) sendTaskResponse(ctx context.Context, peerID peer.ID, resp *pb.TaskResponse) error {
	stream, err := ts.p.NewStream(ctx, peerID, taskResponseProtocolID)
	if err != nil {
		return err
	}
	data, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err = stream.Write(data); err != nil {
		return err
	}
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

func readTaskResponse(s network.Stream) (*pb.TaskResponse, error) {
	data, err := ioutil.ReadAll(s)
	if err != nil {
		return nil, err
	}
	taskResp := &pb.TaskResponse{}
	if err = proto.Unmarshal(data, taskResp); err != nil {
		return nil, err
	}

	return taskResp, nil
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
