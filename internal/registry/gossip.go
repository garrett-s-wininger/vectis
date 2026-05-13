package registry

import (
	"context"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

func (s *registryServer) StartCluster(ctx context.Context) {
	if len(s.opts.PeerAddresses) == 0 {
		return
	}

	if s.log != nil {
		s.log.Info("Starting registry gossip cluster node %s with %d peer(s)", s.opts.NodeID, len(s.opts.PeerAddresses))
	}

	go s.gossipLoop(ctx)
	go s.antiEntropyLoop(ctx)
}

func (s *registryServer) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.closePeerClients()
			return
		case <-ticker.C:
			s.gossipOnce(ctx)
		}
	}
}

func (s *registryServer) antiEntropyLoop(ctx context.Context) {
	ticker := time.NewTicker(s.opts.AntiEntropyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.closePeerClients()
			return
		case <-ticker.C:
			s.antiEntropyOnce(ctx)
		}
	}
}

func (s *registryServer) gossipOnce(ctx context.Context) {
	entries := s.reg.drainDirtyProtoEntries(time.Now())
	if len(entries) == 0 {
		return
	}

	req := &api.GossipRequest{NodeId: &s.opts.NodeID, Entries: entries}
	for _, peer := range s.opts.PeerAddresses {
		peerCtx, cancel := context.WithTimeout(ctx, s.opts.PeerDialTimeout)
		resp, err := s.gossipPeer(peerCtx, peer, req)
		cancel()

		if err != nil {
			s.logPeerFailure("gossip", peer, err)
			continue
		}

		s.reg.mergeProtoEntries(resp.GetEntries(), time.Now())
	}
}

func (s *registryServer) antiEntropyOnce(ctx context.Context) {
	digests := s.reg.digestProtoEntries(time.Now())
	req := &api.RegistrySnapshotRequest{NodeId: &s.opts.NodeID, Digests: digests}
	for _, peer := range s.opts.PeerAddresses {
		peerCtx, cancel := context.WithTimeout(ctx, s.opts.PeerDialTimeout)
		resp, err := s.snapshotPeer(peerCtx, peer, req)
		cancel()

		if err != nil {
			s.logPeerFailure("anti-entropy", peer, err)
			continue
		}

		s.reg.mergeProtoEntries(resp.GetEntries(), time.Now())
	}
}

func (s *registryServer) gossipPeer(ctx context.Context, peer string, req *api.GossipRequest) (*api.GossipResponse, error) {
	client, err := s.peerClient(ctx, peer)
	if err != nil {
		return nil, err
	}

	resp, err := client.GossipOnce(ctx, req)
	if err != nil {
		s.dropPeerClient(peer)
		return nil, err
	}

	return resp, nil
}

func (s *registryServer) snapshotPeer(ctx context.Context, peer string, req *api.RegistrySnapshotRequest) (*api.RegistrySnapshotResponse, error) {
	client, err := s.peerClient(ctx, peer)
	if err != nil {
		return nil, err
	}

	resp, err := client.SnapshotOnce(ctx, req)
	if err != nil {
		s.dropPeerClient(peer)
		return nil, err
	}

	return resp, nil
}

func (s *registryServer) peerClient(ctx context.Context, peer string) (*Registry, error) {
	s.peerMu.Lock()
	client := s.peerClients[peer]
	s.peerMu.Unlock()
	if client != nil {
		return client, nil
	}

	client, err := New(ctx, peer, s.log, interfaces.SystemClock{}, nil)
	if err != nil {
		return nil, err
	}

	s.peerMu.Lock()
	if existing := s.peerClients[peer]; existing != nil {
		s.peerMu.Unlock()
		_ = client.Close()
		return existing, nil
	}
	s.peerClients[peer] = client
	s.peerMu.Unlock()

	return client, nil
}

func (s *registryServer) dropPeerClient(peer string) {
	s.peerMu.Lock()
	client := s.peerClients[peer]
	delete(s.peerClients, peer)
	s.peerMu.Unlock()

	if client != nil {
		_ = client.Close()
	}
}

func (s *registryServer) closePeerClients() {
	s.peerMu.Lock()
	clients := s.peerClients
	s.peerClients = make(map[string]*Registry)
	s.peerMu.Unlock()

	for _, client := range clients {
		_ = client.Close()
	}
}

func (s *registryServer) logPeerFailure(operation, peer string, err error) {
	if s.log == nil {
		return
	}

	s.log.Debug("registry %s with peer %s failed: %v", operation, peer, err)
}
