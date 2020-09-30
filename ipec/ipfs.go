package ipec

import (
	"context"
	"os"

	ipfsFiles "github.com/ipfs/go-ipfs-files"
	ipfsCore "github.com/ipfs/go-ipfs/core"
	ipfsCoreAPI "github.com/ipfs/go-ipfs/core/coreapi"
	ipfsCoreIface "github.com/ipfs/interface-go-ipfs-core"
	ipfsPath "github.com/ipfs/interface-go-ipfs-core/path"
	log "github.com/sirupsen/logrus"
)

type IpfsService struct {
	node *Node
	ipfsCoreIface.CoreAPI
	ipfsNode *ipfsCore.IpfsNode
}

func NewIpfsService(ctx context.Context, node *Node) *IpfsService {
	// TODO: figure out what build config should we put here
	ipfsNode, err := ipfsCore.NewNode(ctx, &ipfsCore.BuildCfg{
		Online: true,
	})

	if err != nil {
		panic(err)
	}

	log.WithField("service", "IPFS").Infof(
		"Init IPFS service - PeerID: %v, Address: %v",
		ipfsNode.PeerHost.ID().Pretty(),
		ipfsNode.PeerHost.Addrs(),
	)

	api, err := ipfsCoreAPI.NewCoreAPI(ipfsNode)
	if err != nil {
		panic(err)
	}

	go func() {
		select {
		case <-ctx.Done():
			ipfsNode.Close()

			log.WithField("service", "IPFS").Infof(
				"Closed IPFS service - PeerID: %v", ipfsNode.PeerHost.ID().Pretty(),
			)
		}
	}()

	return &IpfsService{
		node:     node,
		CoreAPI:  api,
		ipfsNode: ipfsNode,
	}
}

func (s *IpfsService) Upload(ctx context.Context, path string) (ipfsPath.Resolved, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	node, err := ipfsFiles.NewSerialFile(path, true, stat)
	if err != nil {
		return nil, err
	}

	log.WithField("ipfs_host", s.ipfsNode.PeerHost.ID().Pretty()).Infof(
		"Start uploading file(s) at file system path %s", path,
	)

	p, err := s.Unixfs().Add(ctx, node)
	if err != nil {
		return nil, err
	}

	log.WithField("ipfs_host", s.ipfsNode.PeerHost.ID().Pretty()).Infof(
		"Uploaded file(s) at file system path %s - IPFS path: %s", path, p.String(),
	)

	return p, nil
}

func (s *IpfsService) Download(ctx context.Context, p ipfsPath.Resolved, writePath string) error {
	log.WithField("ipfs_host", s.ipfsNode.PeerHost.ID().Pretty()).Infof(
		"Start downloading file(s) at IPFS path %s - file system path: %s", p.String(), writePath,
	)

	node, err := s.Unixfs().Get(ctx, p)
	if err != nil {
		return err
	}

	err = ipfsFiles.WriteTo(node, writePath)
	if err != nil {
		return err
	}

	log.WithField("ipfs_host", s.ipfsNode.PeerHost.ID().Pretty()).Infof(
		"Downloaded file(s) at IPFS path %s - file system path: %s", p.String(), writePath,
	)
	return nil
}

func (s *IpfsService) AddPin(ctx context.Context, p ipfsPath.Resolved) error {
	if err := s.Pin().Add(ctx, p); err != nil {
		return err
	}

	log.WithField("ipfs_host", s.ipfsNode.PeerHost.ID().Pretty()).Infof(
		"Added Pin - Path: %s", p.String(),
	)

	return nil
}

func (s *IpfsService) RemovePin(ctx context.Context, p ipfsPath.Resolved) error {
	if err := s.Pin().Rm(ctx, p); err != nil {
		return err
	}

	log.WithField("ipfs_host", s.ipfsNode.PeerHost.ID().Pretty()).Infof(
		"Removed Pin - Path: %s", p.String(),
	)

	return nil
}
