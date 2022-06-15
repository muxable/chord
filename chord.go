package chord

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const M = 64

type Node interface {
	ID() uint64
	Host() string
	Successor() (Node, error)
	Predecessor() (Node, error)
	FindSuccessor(uint64) (Node, error)
	ClosestPrecedingNode(uint64) (Node, error)
	Notify(Node) error
	Serialize() string
}

type LocalNode struct {
	ctx         context.Context
	id          uint64
	host        string
	finger      [M]Node
	next        uint16
	predecessor Node
}

var _ Node = (*LocalNode)(nil)

func NewLocalNode(ctx context.Context, id uint64, host string) *LocalNode {
	n := &LocalNode{ctx: ctx, id: id, host: host}
	go func() {
		stabilize := time.NewTicker(1 * time.Second)
		fixFingers := time.NewTicker(100 * time.Millisecond)
		checkPredecessor := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-n.ctx.Done():
				stabilize.Stop()
				return
			case <-stabilize.C:
				if err := n.Stabilize(); err != nil {
					log.Printf("got error %v", err)
				}
			case <-fixFingers.C:
				if err := n.FixFingers(); err != nil {
					log.Printf("got error %v", err)
				}
			case <-checkPredecessor.C:
				n.CheckPredecessor()
			}
		}
	}()
	return n
}

func (n *LocalNode) ID() uint64 {
	return n.id
}

func (n *LocalNode) Host() string {
	return n.host
}

func (n *LocalNode) Successor() (Node, error) {
	if n.finger[0] == nil {
		return n, nil
	}
	return n.finger[0], nil
}

func (n *LocalNode) Predecessor() (Node, error) {
	return n.predecessor, nil
}

func (n *LocalNode) FindSuccessor(id uint64) (Node, error) {
	successor, err := n.Successor()
	if err != nil {
		return nil, err
	}
	if id > n.ID() && id < successor.ID() {
		return successor, nil
	} else {
		// forward the query around the circle.
		n0, err := n.ClosestPrecedingNode(id)
		if err != nil {
			return nil, err
		}
		if n0 == n {
			return n, nil
		}
		return n0.FindSuccessor(id)
	}
}

func (n *LocalNode) ClosestPrecedingNode(id uint64) (Node, error) {
	for i := M - 1; i >= 0; i-- {
		if n.finger[i] == nil {
			continue
		}
		if n.ID() < n.finger[i].ID() && n.finger[i].ID() < id {
			return n.finger[i], nil
		}
	}
	return n, nil
}

func (n *LocalNode) Join(m Node) error {
	s, err := m.FindSuccessor(n.id)
	if err != nil {
		return err
	}
	n.predecessor = nil
	n.finger[0] = s
	return s.Notify(n)
}

func (n *LocalNode) Stabilize() error {
	successor, err := n.Successor()
	if err != nil {
		return err
	}
	x, err := successor.Predecessor()
	if err != nil {
		return err
	}
	if x == nil {
		return nil
	}
	if n.ID() < x.ID() && x.ID() < successor.ID() {
		n.finger[0] = x
	}
	return successor.Notify(n)
}

func (n *LocalNode) Notify(m Node) error {
	predecessor, err := n.Predecessor()
	if err != nil {
		return err
	}
	if predecessor == nil || (predecessor.ID() < m.ID() && m.ID() < n.ID()) {
		n.predecessor = m
	}
	return nil
}

func (n *LocalNode) FixFingers() error {
	s, err := n.FindSuccessor(n.ID() + (1 << n.next))
	if err != nil {
		return err
	}
	n.finger[n.next] = s
	n.next = (n.next + 1) % M
	return nil
}

func (n *LocalNode) CheckPredecessor() {
	switch p := n.predecessor.(type) {
	case *RemoteNode:
		if m, err := p.op("", ""); err != nil || m.Host() != n.predecessor.Host() {
			n.predecessor = nil
		}
	}
}

func (n *LocalNode) HTTPHandlerFunc() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("op") {
		case "Successor":
			successor, err := n.Successor()
			if err != nil {
				w.WriteHeader(500)
			} else {
				w.Write([]byte(successor.Serialize()))
			}
		case "Predecessor":
			predecessor, err := n.Predecessor()
			if err != nil {
				w.WriteHeader(500)
			} else if predecessor == nil {
				w.WriteHeader(200)
			} else {
				w.Write([]byte(predecessor.Serialize()))
			}
		case "FindSuccessor":
			id, err := strconv.ParseUint(r.URL.Query().Get("id"), 16, 64)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			m, err := n.FindSuccessor(id)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			w.Write([]byte(m.Serialize()))
		case "ClosestPrecedingNode":
			id, err := strconv.ParseUint(r.URL.Query().Get("id"), 16, 64)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			m, err := n.ClosestPrecedingNode(id)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			w.Write([]byte(m.Serialize()))
		case "Notify":
			id, err := strconv.ParseUint(r.URL.Query().Get("id"), 16, 64)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			if err := n.Notify(&RemoteNode{id: id, host: r.URL.Query().Get("host")}); err != nil {
				w.WriteHeader(400)
				return
			}
			w.WriteHeader(200)
		default:
			w.Write([]byte(n.Serialize()))
		}
	})
}

func (n *LocalNode) Serialize() string {
	return fmt.Sprintf("%x:%s", n.id, n.host)
}

func (n *LocalNode) String() string {
	return fmt.Sprintf("local[%s]", n.Serialize())
}

type RemoteNode struct {
	id   uint64
	host string
}

var _ Node = (*RemoteNode)(nil)

func NewRemoteNode(addr string) (*RemoteNode, error) {
	// resolve the id automatically.
	resp, err := http.Get(fmt.Sprintf("http://%s/node", addr))
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	n := &RemoteNode{}
	return n, n.Deserialize(string(body))
}

func (n *RemoteNode) ID() uint64 {
	return n.id
}

func (n *RemoteNode) Host() string {
	return n.host
}

func (n *RemoteNode) op(name string, arg string) (Node, error) {
	url := fmt.Sprintf("http://%s/node?op=%s", n.host, name)
	if arg != "" {
		url += fmt.Sprintf("&%s", arg)
	}
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	m := &RemoteNode{}
	return m, m.Deserialize(string(body))
}

func (n *RemoteNode) Successor() (Node, error) {
	return n.op("Successor", "")
}

func (n *RemoteNode) Predecessor() (Node, error) {
	return n.op("Predecessor", "")
}

func (n *RemoteNode) FindSuccessor(id uint64) (Node, error) {
	return n.op("FindSuccessor", fmt.Sprintf("id=%x", id))
}

func (n *RemoteNode) ClosestPrecedingNode(id uint64) (Node, error) {
	return n.op("ClosestPrecedingNode", fmt.Sprintf("id=%x", id))
}

func (n *RemoteNode) Notify(m Node) error {
	_, err := n.op("Notify", fmt.Sprintf("id=%x&host=%s", m.ID(), m.Host()))
	return err
}

func (n *RemoteNode) Serialize() string {
	return fmt.Sprintf("%x:%s", n.id, n.host)
}

func (n *RemoteNode) Deserialize(s string) error {
	if len(s) < 16 {
		return nil
	}
	tokens := strings.SplitN(s, ":", 2)
	id, err := strconv.ParseUint(tokens[0], 16, 64)
	if err != nil {
		return err
	}
	n.id = id
	n.host = tokens[1]
	return nil
}

func (n *RemoteNode) String() string {
	return fmt.Sprintf("remote[%s]", n.Serialize())
}