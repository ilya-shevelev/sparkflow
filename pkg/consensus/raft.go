// Package consensus provides Raft-based distributed consensus for
// leader election and state replication using hashicorp/raft.
package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Config configures the Raft consensus node.
type Config struct {
	NodeID       string
	BindAddr     string
	DataDir      string
	Bootstrap    bool
	Peers        []PeerConfig
	Logger       *slog.Logger
}

// PeerConfig describes a peer in the Raft cluster.
type PeerConfig struct {
	ID   string
	Addr string
}

// Command represents a state machine command.
type Command struct {
	Type    string `json:"type"`
	Key     string `json:"key"`
	Value   []byte `json:"value"`
}

// Node wraps a Raft consensus node with application-level operations.
type Node struct {
	raft   *raft.Raft
	fsm    *FSM
	config Config
	logger *slog.Logger
}

// NewNode creates and initializes a new Raft consensus node.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	fsm := NewFSM()

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.SnapshotInterval = 60 * time.Second
	raftConfig.SnapshotThreshold = 1024

	// Suppress default raft logging.
	raftConfig.LogOutput = io.Discard

	// Set up transport.
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve bind addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create TCP transport: %w", err)
	}

	// Set up storage.
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("create log store: %w", err)
	}

	stableStorePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		return nil, fmt.Errorf("create stable store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(cfg.BindAddr),
			},
		}
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.Addr),
			})
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil {
			// Ignore already-bootstrapped errors.
			cfg.Logger.Debug("bootstrap", slog.Any("result", err))
		}
	}

	return &Node{
		raft:   r,
		fsm:    fsm,
		config: cfg,
		logger: cfg.Logger,
	}, nil
}

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// Apply submits a command to the Raft log.
func (n *Node) Apply(cmd Command, timeout time.Duration) error {
	if !n.IsLeader() {
		return fmt.Errorf("not the leader; leader is at %s", n.LeaderAddr())
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	f := n.raft.Apply(data, timeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("apply command: %w", err)
	}

	return nil
}

// Get reads a value from the FSM state.
func (n *Node) Get(key string) ([]byte, bool) {
	return n.fsm.Get(key)
}

// AddPeer adds a new node to the Raft cluster.
func (n *Node) AddPeer(id, addr string) error {
	if !n.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}

// RemovePeer removes a node from the Raft cluster.
func (n *Node) RemovePeer(id string) error {
	if !n.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	f := n.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	return f.Error()
}

// Shutdown gracefully shuts down the Raft node.
func (n *Node) Shutdown() error {
	f := n.raft.Shutdown()
	return f.Error()
}

// Stats returns Raft statistics.
func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

// FSM implements the raft.FSM interface for state machine operations.
type FSM struct {
	mu    sync.RWMutex
	state map[string][]byte
}

// NewFSM creates a new finite state machine.
func NewFSM() *FSM {
	return &FSM{
		state: make(map[string][]byte),
	}
}

// Apply applies a Raft log entry to the FSM.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case "set":
		f.state[cmd.Key] = cmd.Value
	case "delete":
		delete(f.state, cmd.Key)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	return nil
}

// Snapshot returns a snapshot of the FSM state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	state := make(map[string][]byte, len(f.state))
	for k, v := range f.state {
		cp := make([]byte, len(v))
		copy(cp, v)
		state[k] = cp
	}

	return &fsmSnapshot{state: state}, nil
}

// Restore restores the FSM from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state map[string][]byte
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
	return nil
}

// Get reads a value from the state.
func (f *FSM) Get(key string) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.state[key]
	return v, ok
}

type fsmSnapshot struct {
	state map[string][]byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
