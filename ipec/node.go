package ipec

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/DrakeW/go-ipec/pb"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/sirupsen/logrus"
)

// Node - represents a node in the network
type Node struct {
	host.Host
	ts *TaskService
}

// NewNode - create a new ndoe
func NewNode(ctx context.Context, h host.Host) *Node {
	n := &Node{Host: h}
	ts := NewTaskService(ctx, n)
	n.ts = ts
	return n
}

// HandleTaskRequest - Implements HandleTaskRequest of TaskPerformer
func (n *Node) HandleTaskRequest(req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.WithField("task", req.Task.TaskId).Info("Start handling task")

	taskDir, err := setupTaskDir(req.Task)
	if err != nil {
		return nil, err
	}

	if err = executeTask(taskDir); err != nil {
		return nil, err
	}

	output, err := ioutil.ReadFile(filepath.Join(taskDir, "output"))
	if err != nil {
		return nil, err
	}

	return &pb.TaskResponse{
		Status:      pb.TaskResponse_DONE,
		TaskId:      req.Task.TaskId,
		Output:      output,
		FinishedAt:  time.Now().Unix(),
		PerformerId: n.ID().Pretty(),
	}, nil
}

// CreateTask - Implements CreateTask of TaskOwner
func (n *Node) CreateTask(function, input []byte, description string) *pb.Task {
	return &pb.Task{
		TaskId:      uuid.New().String(),
		Function:    function,
		Input:       input,
		Description: description,
		OwnerId:     n.ID().Pretty(),
		CreatedAt:   time.Now().Unix(),
	}
}

// Dispatch - dispatch a task to the network and return the task performer peer
func (n *Node) Dispatch(ctx context.Context, task *pb.Task) peer.ID {
	req := &pb.TaskRequest{
		Task:     task,
		SenderId: n.ID().Pretty(),
	}

	peers := n.Peerstore().Peers()
	acceptC := make(chan peer.ID, 1)
	defer close(acceptC)

	log.WithField("task", task.TaskId).Infof("Dispatching tasks to connected peers %s", peers)

	peerToChosenC := make(map[peer.ID]chan bool)
	var once sync.Once
	for _, p := range peers {
		go func(peer peer.ID) {
			peerToChosenC[peer] = make(chan bool, 1)
			if err := n.ts.Dispatch(ctx, peer, req, peerToChosenC[peer]); err != nil {
				return
			}

			once.Do(func() {
				acceptC <- peer
			})
		}(p)
	}

	select {
	case selectedPeer := <-acceptC:
		// first one responded is the chosen one to perform the task
		for peer, c := range peerToChosenC {
			c <- peer == selectedPeer
		}
		return selectedPeer
	case <-ctx.Done():
		return ""
	}
}

// HandleTaskResponse - Implements HandleTaskResponse of TaskOwner
func (n *Node) HandleTaskResponse(resp *pb.TaskResponse) error {
	// TODO: implement something else
	log.WithField("from", resp.PerformerId).Info("Received response")
	log.WithField("from", resp.PerformerId).Infof("Response: %s", resp)
	return nil
}

func setupTaskDir(task *pb.Task) (string, error) {
	dir, err := ioutil.TempDir("", fmt.Sprintf("%s-", task.TaskId))
	if err != nil {
		return "", err
	}

	if err = ioutil.WriteFile(filepath.Join(dir, "func"), task.Function, os.FileMode(0544)); err != nil {
		return dir, err
	}
	if err = ioutil.WriteFile(filepath.Join(dir, "input"), task.Input, os.FileMode(0444)); err != nil {
		return dir, err
	}
	if err = ioutil.WriteFile(filepath.Join(dir, "output"), []byte{}, os.FileMode(0644)); err != nil {
		return dir, err
	}

	return dir, nil
}

func executeTask(taskDir string) error {
	inputFile, err := os.Open(filepath.Join(taskDir, "input"))
	if err != nil {
		return err
	}

	outputFile, err := os.OpenFile(filepath.Join(taskDir, "output"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	cmd := exec.Command(filepath.Join(taskDir, "func"))
	cmd.Stdin = inputFile
	cmd.Stdout = outputFile

	if err = cmd.Run(); err != nil {
		return err
	}
	return nil
}
