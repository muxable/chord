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

const R = 32

func between(n1, n2, n3 uint64) bool {
	if n1 < n3 {
		return n1 < n2 && n2 < n3
	}
	return n1 < n2 || n2 < n3
}

type Node interface {
	ID() uint64
	Host() string
	Successors() ([R]Node, error)
	Predecessor() (Node, error)
	FindSuccessor(uint64) (Node, error)
	Notify(Node) error
	Serialize() string
}

type LocalNode struct {
	ctx         context.Context
	id          uint64
	host        string
	finger      [M]Node
	next        uint16
	successors  [R]Node
	predecessor Node
}

var _ Node = (*LocalNode)(nil)

func NewLocalNode(ctx context.Context, id uint64, host string) *LocalNode {
	n := &LocalNode{ctx: ctx, id: id, host: host}
	for i := 0; i < M; i++ {
		n.finger[i] = n
	}
	n.predecessor = n
	go func() {
		stabilize := time.NewTicker(1 * time.Second)
		fixFingers := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-n.ctx.Done():
				stabilize.Stop()
				return
			case <-stabilize.C:
				if err := n.Stabilize(); err != nil {
					for i := 0; i < R - 1; i++ {
						n.successors[i] = n.successors[i + 1]
					}
				}
			case <-fixFingers.C:
				if err := n.FixFingers(); err != nil {
					log.Printf("got error %v", err)
				}
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

func (n *LocalNode) Successors() ([R]Node, error) {
	return n.successors, nil
}

func (n *LocalNode) Predecessor() (Node, error) {
	return n.predecessor, nil
}

func (n *LocalNode) FindSuccessor(id uint64) (Node, error) {
	successors, err := n.Successors()
	if err != nil {
		return nil, err
	}
	if n.ID() < id && id <= successors[0].ID() {
		return successors[0], nil
	} else {
		// forward the query around the circle.
		return n.ClosestPrecedingNode(id).FindSuccessor(id)
	}
}

func (n *LocalNode) ClosestPrecedingNode(id uint64) Node {
	for i := M - 1; i >= 0; i-- {
		if n.ID() < n.finger[i].ID() && n.finger[i].ID() < id {
			return n.finger[i]
		}
	}
	return n
}

func (n *LocalNode) Join(m Node) (error) {
	n.predecessor = nil
	s, err := m.FindSuccessor(n.id)
	if err != nil {
		return err
	}
	t, err := s.Successors()
	if err != nil {
		return err
	}
	n.successors[0] = s
	if copy(n.successors[1:], t[:(R-1)]) != R-1 {
		return io.ErrShortWrite
	}
	return nil
}

func (n *LocalNode) Stabilize() error {
	x, err := n.successors[0].Predecessor()
	if err != nil {
		return err
	}
	y, err := n.successors[0].Successors()
	if err != nil {
		return err
	}
	if copy(n.successors[1:], y[:(R-1)]) != R-1 {
		return io.ErrShortWrite
	}
	if (between(n.ID(), x.ID(), n.successors[0].ID())) {
		 z, err := x.Successors()
		if err != nil {
			return err
		}
		n.successors[0] = x
		if copy(n.successors[1:], z[:(R-1)]) != R-1 {
			return io.ErrShortWrite
		}
	}
	return n.successors[0].Notify(n)
}

func (n *LocalNode) Notify(m Node) error {
	if n.predecessor == nil {
		n.predecessor = m
	} else {
		switch p := n.predecessor.(type) {
		case *RemoteNode:
			if m, err := p.op("", ""); err != nil || m.Host() != n.predecessor.Host() ||  between(n.predecessor.ID(), m.ID(), n.ID()) {
				n.predecessor = m
			}
		}
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

func (n *LocalNode) HTTPHandlerFunc() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("op") {
		case "Successors":
			successors, err := n.Successors()
			if err != nil {
				w.WriteHeader(500)
			} else {
				for i := 0; i < len(successors); i++ {
					w.Write([]byte(successors[i].Serialize()))
					if i != len(successors) - 1 {
						w.Write([]byte("\n"))
					}
				}
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

func (n *RemoteNode) op(name string, arg string) ([]string, error) {
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
	tokens := strings.Split(string(body), "\n")
	return tokens, nil
}

func (n *RemoteNode) Successors() ([R]Node, error) {
	res := [R]Node{}
	tokens, err := n.op("Successors", "")
	if err != nil {
		return res, err
	}
	for i := 0; i < R; i++ {
		m := &RemoteNode{}
		if err := m.Deserialize(tokens[i]); err != nil {
			return res, err
		}
		res[i] = m
	}
	return res, nil
}

func (n *RemoteNode) Predecessor() (Node, error) {
	tokens, err := n.op("Predecessor", "")
	if err != nil {
		return nil, err
	}
	m := &RemoteNode{}
	return m, m.Deserialize(tokens[0])
}

func (n *RemoteNode) FindSuccessor(id uint64) (Node, error) {
	tokens, err := n.op("FindSuccessor", "")
	if err != nil {
		return nil, err
	}
	m := &RemoteNode{}
	return m, m.Deserialize(tokens[0])
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
