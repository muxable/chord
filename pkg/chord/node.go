package chord

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
)

const M = 64

type Node interface {
	Id() uint64
	Successor() (Node, error)
	Predecessor() (Node, error)
	FindSuccessor(uint64) (Node, error)
	ClosestPrecedingNode(uint64) (Node, error)
	Notify(Node) error
	Serialize() string
}

type LocalNode struct {
	id          uint64
	finger      [M]Node
	predecessor Node
	successor   Node
}

var _ Node = (*LocalNode)(nil)

func (n *LocalNode) Id() uint64 {
	return n.id
}

func (n *LocalNode) Successor() (Node, error) {
	if n.successor == nil {
		return n, nil
	}
	return n.successor, nil
}

func (n *LocalNode) Predecessor() (Node, error) {
	return n.predecessor, nil
}

func (n *LocalNode) FindSuccessor(id uint64) (Node, error) {
	successor := n.successor
	if id > n.Id() && id < successor.Id() {
		return successor, nil
	} else {
		// forward the query around the circle.
		n0, err := n.ClosestPrecedingNode(id)
		if err != nil {
			return nil, err
		}
		return n0.FindSuccessor(id)
	}
}

func (n *LocalNode) ClosestPrecedingNode(id uint64) (Node, error) {
	for i := M; i >= 1; i-- {
		if n.Id() < n.finger[i].Id() && n.finger[i].Id() < id {
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
	n.successor = s
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
	if n.Id() < x.Id() && x.Id() < successor.Id() {
		n.successor = x
	}
	successor.Notify(n)
}

func (n *LocalNode) Notify(m Node) error {
	predecessor, err := n.Predecessor()
	if err != nil {
		return err
	}
	if predecessor == nil || (predecessor.Id() < m.Id() && m.Id() < n.Id()) {
		n.predecessor = m
	}
	return nil
}

func (n *LocalNode) HTTPHandlerFunc() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("op") {
		case "Successor":
			w.Write([]byte(n.successor.Serialize()))
		case "Predecessor":
			w.Write([]byte(n.successor.Serialize()))
		case "FindSuccessor":
			arg, err := strconv.ParseUint(r.URL.Query().Get("arg"), 16, 64)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			m, err := n.FindSuccessor(arg)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			w.Write([]byte(m.Serialize()))
		case "ClosestPrecedingNode":
			arg, err := strconv.ParseUint(r.URL.Query().Get("arg"), 16, 64)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			m, err := n.ClosestPrecedingNode(arg)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			w.Write([]byte(m.Serialize()))
		case "Notify":
			arg, err := strconv.ParseUint(r.URL.Query().Get("arg"), 16, 64)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			if err := n.Notify(&RemoteNode{arg}); err != nil {
				w.WriteHeader(400)
				return
			}
			w.Write([]byte(m.Serialize()))
		}
	})
}

func (n *LocalNode) Serialize() string {
	return fmt.Sprintf("%x", n.Id())
}

type RemoteNode struct {
	id      uint64
	baseURL string
}

var _ Node = (*RemoteNode)(nil)

func (n *RemoteNode) Id() uint64 {
	return n.id
}

func (n *RemoteNode) op(name string, arg uint64) (Node, error) {
	resp, err := http.Get(fmt.Sprintf("%s?op=%s&arg=%x", n.baseURL, name, arg))
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
	return n.op("Successor", 0)
}

func (n *RemoteNode) Predecessor() (Node, error) {
	return n.op("Predecessor", 0)
}

func (n *RemoteNode) FindSuccessor(id uint64) (Node, error) {
	return n.op("FindSuccessor", id)
}

func (n *RemoteNode) ClosestPrecedingNode(id uint64) (Node, error) {
	return n.op("ClosestPrecedingNode", id)
}

func (n *RemoteNode) Notify(m Node) error {
	_, err := n.op("ClosestPrecedingNode", m.Id())
	return err
}

func (n *RemoteNode) Serialize() string {
	return fmt.Sprintf("%x%s", n.id, n.baseURL)
}

func (n *RemoteNode) Deserialize(s string) error {
	if len(s) < 16 {
		return nil
	}
	if len(s) >= 16 {
		id, err := strconv.ParseUint(s[:16], 16, 64)
		if err != nil {
			return err
		}
		n.id = id
	}
	if len(s) > 16 {
		n.baseURL = string(s[16:])
	}
	return nil
}
