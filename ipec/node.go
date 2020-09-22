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

	"github.com/DrakeW/go-ipec/ipec/pb"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/sirupsen/logrus"
)

// Node - represents a node in the network
type Node struct {
	host.Host
	ts *TaskService
	fs *IpfsService
}

// NewNodeWithHost - create a new node with a libp2p host
func NewNodeWithHost(ctx context.Context, h host.Host) *Node {
	n := &Node{Host: h}
	n.ts = NewTaskService(ctx, n)
	n.fs = NewIpfsService(ctx, n)
	return n
}

// HandleTaskRequest - Implements HandleTaskRequest of TaskPerformer
func (n *Node) HandleTaskRequest(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.WithField("task", req.Task.TaskId).Info("Start handling task")

	taskDir, err := n.downloadTask(ctx, req.Task)
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
func (n *Node) CreateTask(ctx context.Context, funcPath, inputPath, description string) (*pb.Task, error) {

	taskID := uuid.New().String()
	taskDir, err := setupTaskDir(taskID, funcPath, inputPath)
	if err != nil {
		return nil, err
	}

	path, err := n.fs.Upload(ctx, taskDir)
	if err != nil {
		return nil, err
	}

	if err = n.fs.AddPin(ctx, path); err != nil {
		return nil, err
	}

	return &pb.Task{
		TaskId:      taskID,
		Cid:         path.Cid().String(),
		Description: description,
		OwnerId:     n.ID().Pretty(),
		CreatedAt:   time.Now().Unix(),
	}, nil
}

// Dispatch - dispatch a task to the network and return the task performer peer
func (n *Node) Dispatch(ctx context.Context, task *pb.Task) peer.ID {
	req := &pb.TaskRequest{
		Task:     task,
		SenderId: n.ID().Pretty(),
	}

	// TODO: (junyu) need a module to determine which peer to connect to based on heuritsitcs like
	// RTT, computing power (cpu cores, mem, etc), availability
	peers := n.Peerstore().Peers()

	acceptC := make(chan peer.ID, 1)
	defer close(acceptC)

	log.WithField("task", task.TaskId).Infof("Dispatching tasks to connected peers %s except self", peers)

	peerToChosenC := make(map[peer.ID]chan bool)
	var once sync.Once
	for _, p := range peers {
		if p == n.ID() {
			continue
		}

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
		log.Warnf("No peer was selected to execute task %s. Dispatch failed.")
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

func setupTaskDir(taskID, funcPath, inputPath string) (string, error) {
	dir, err := ioutil.TempDir("", fmt.Sprintf("task-%s-", taskID))
	if err != nil {
		return "", err
	}

	funcData, err := ioutil.ReadFile(funcPath)
	if err != nil {
		return "", nil
	}

	inputData, err := ioutil.ReadFile(inputPath)
	if err != nil {
		return "", nil
	}

	if err = ioutil.WriteFile(filepath.Join(dir, "func"), funcData, os.FileMode(0544)); err != nil {
		return dir, err
	}
	if err = ioutil.WriteFile(filepath.Join(dir, "input"), inputData, os.FileMode(0444)); err != nil {
		return dir, err
	}
	if err = ioutil.WriteFile(filepath.Join(dir, "output"), []byte{}, os.FileMode(0644)); err != nil {
		return dir, err
	}

	return dir, nil
}

func (n *Node) downloadTask(ctx context.Context, task *pb.Task) (string, error) {
	c, err := cid.Parse(task.Cid)
	if err != nil {
		return "", err
	}

	taskDirPath := filepath.Join(os.TempDir(), fmt.Sprintf("task-%s-perform", task.TaskId))

	if err = n.fs.Download(ctx, path.IpfsPath(c), taskDirPath); err != nil {
		return "", err
	}
	return taskDirPath, nil
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
