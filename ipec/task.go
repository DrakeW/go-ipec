package ipec

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/DrakeW/go-ipec/ipec/pb"
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
var taskAcceptProtocolID protocol.ID = "/ipec/task/accept/0.1.0"

// TaskService - handles the communication between TaskOwner and TaskPerformers
type TaskService struct {
	p                  TaskOwnerPerformer
	peerToTaskResultC  map[peer.ID]chan *pb.TaskResponse // a map of task owner to a channel of task result which will be notified when task result is ready
	newTaskownerC      chan peer.ID                      // channel to notify this peer starts handling a new task from a specific task owner
	taskToPerformerMap map[string]peer.ID                // keeps track of which performer handles which task
	pendingAckTasks    map[string]*pb.TaskRequest
}

// NewTaskService - create new task service and register its stream handlers
func NewTaskService(ctx context.Context, p TaskOwnerPerformer) *TaskService {
	ts := &TaskService{
		p:                  p,
		peerToTaskResultC:  make(map[peer.ID]chan *pb.TaskResponse),
		newTaskownerC:      make(chan peer.ID),
		pendingAckTasks:    make(map[string]*pb.TaskRequest),
		taskToPerformerMap: make(map[string]peer.ID),
	}
	ts.p.SetStreamHandler(taskRequestProtocolID, ts.handleTaskRequest)
	ts.p.SetStreamHandler(taskResponseProtocolID, ts.handleTaskResponse)
	ts.p.SetStreamHandler(taskAcceptProtocolID, ts.handleTaskAcceptACK)

	go ts.loop(ctx)

	return ts
}

func (ts *TaskService) loop(ctx context.Context) {
	for {
		select {
		case newPeer := <-ts.newTaskownerC:
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
		case resp := <-ts.peerToTaskResultC[peerID]:
			if err := ts.sendTaskResponse(ctx, peerID, resp); err != nil {
				log.WithField("to", peerID).Errorf("Failed to send task response - Error: %s", err.Error())
			}
			close(ts.peerToTaskResultC[peerID])
			return
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
	stream.Close()

	// read Accept response from peer
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

	go func() {
		select {
		case chosen := <-chosenC:
			ts.sendAccpetAckMessage(ctx, peer, req.Task.TaskId, chosen)
			close(chosenC)
		}
	}()

	return nil
}

// sendAccpetAckMessage - writes a ACK message to the performer peer who accepted
// the task and was chosen to perform the task with taskID by the task owner
func (ts *TaskService) sendAccpetAckMessage(
	ctx context.Context, peer peer.ID, taskID string, chosen bool,
) error {
	log.WithFields(log.Fields{
		"to":     peer,
		"task":   taskID,
		"chosen": chosen,
	}).Infof("Sending task accept ACK")
	s, err := ts.p.NewStream(ctx, peer, taskAcceptProtocolID)
	if err != nil {
		return err
	}
	acceptAck := &pb.TaskAcceptAck{
		TaskId: taskID,
		Chosen: chosen,
	}

	data, err := proto.Marshal(acceptAck)
	if err != nil {
		return err
	}

	s.Write(data)
	s.Close()

	if chosen {
		ts.taskToPerformerMap[taskID] = peer
	}
	return nil
}

func (ts *TaskService) handleTaskRequest(s network.Stream) {
	taskReq, err := readTaskRequest(s)
	if err != nil {
		s.Reset()
		return
	}
	log.WithFields(log.Fields{
		"from": taskReq.Task.OwnerId,
		"task": taskReq.Task.TaskId,
	}).Infof("Received task request")

	if err = writeTaskReqAccept(s, taskReq.Task.TaskId); err != nil {
		s.Reset()
		return
	}
	log.WithFields(log.Fields{
		"from": taskReq.Task.OwnerId,
		"task": taskReq.Task.TaskId,
	}).Infof("Accepted task. Pending ACK")

	ts.pendingAckTasks[taskReq.Task.TaskId] = taskReq
	s.Close()
}

func (ts *TaskService) handleTaskAcceptACK(s network.Stream) {
	// - if task ID is returned then this peer is chosen to perform the task
	// - if "na" is returned then this peer is NOT chosen
	data, err := ioutil.ReadAll(s)
	if err != nil {
		log.Error("Failed to read ACK response from Task owner")
		s.Reset()
		return
	}

	ackResp := &pb.TaskAcceptAck{}
	if err = proto.Unmarshal(data, ackResp); err != nil {
		log.Error("Failed to parse ACK response")
		s.Reset()
	}

	chosen := ackResp.Chosen
	taskID := ackResp.TaskId

	if chosen == false {
		log.WithField("task", taskID).Info("Received ACK. This peer is not chosen as the task performer")
		delete(ts.pendingAckTasks, taskID)
		return
	}

	log.WithField("task", taskID).Infof("Received ACK. This peer is chosen as the task performer")

	taskReq := ts.pendingAckTasks[taskID]
	taskOwnerID, err := peer.Decode(taskReq.Task.OwnerId)
	if err != nil {
		log.WithField("ownerId", taskReq.Task.OwnerId).Errorf("Failed to decode task owner id")
	}
	if _, ok := ts.peerToTaskResultC[taskOwnerID]; !ok {
		ts.peerToTaskResultC[taskOwnerID] = make(chan *pb.TaskResponse, 1)
		ts.newTaskownerC <- taskOwnerID
	}

	go func() {
		resp, err := ts.p.HandleTaskRequest(taskReq)
		if err != nil {
			resp = &pb.TaskResponse{
				Status:      pb.TaskResponse_FAILED,
				TaskId:      taskReq.Task.TaskId,
				Output:      []byte(err.Error()),
				PerformerId: ts.p.ID().Pretty(),
			}
		}
		ts.peerToTaskResultC[taskOwnerID] <- resp
	}()
}

// TODO: implement this more properly
func (ts *TaskService) handleTaskResponse(s network.Stream) {
	resp, err := readTaskResponse(s)
	if err != nil {
		s.Reset()
		return
	}

	go ts.p.HandleTaskResponse(resp)
	s.Close()
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
	stream.Close()

	log.WithField("to", peerID).Infof("Sent task response")
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

func writeTaskReqAccept(s network.Stream, taskID string) error {
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
