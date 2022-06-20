package chord

import (
	"context"
	"errors"
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
const R = 4 // 32 for production

func between(n1, n2, n3 uint64) bool {
	if n1 < n3 {
		return n1 < n2 && n2 <= n3
	}
	return n1 < n2 || n2 <= n3
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
	id            uint64
	host          string
	finger        [M]Node
	successors    [R]Node
	predecessor   Node
	onPredecessor func(Node)
}

var _ Node = (*LocalNode)(nil)

func NewLocalNode(id uint64, host string, m Node) (*LocalNode, error) {
	n := &LocalNode{id: id, host: host}
	for i := 0; i < M; i++ {
		n.finger[i] = n
	}
	for i := 0; i < R; i++ {
		n.successors[i] = n
	}
	if m == nil {
		n.predecessor = n
	} else {
		s, err := m.FindSuccessor(n.id)
		if err != nil {
			return nil, err
		}
		t, err := s.Successors()
		if err != nil {
			return nil, err
		}
		n.successors[0] = s
		if copy(n.successors[1:], t[:(R-1)]) != R-1 {
			return nil, io.ErrShortWrite
		}
	}
	return n, nil
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
	if between(n.ID(), id, successors[0].ID()) {
		return successors[0], nil
	} else {
		// forward the query around the circle.
		return n.ClosestPrecedingNode(id).FindSuccessor(id)
	}
}

func (n *LocalNode) ClosestPrecedingNode(id uint64) Node {
	for i := M - 1; i >= 0; i-- {
		if between(n.ID(), n.finger[i].ID(), id) {
			return n.finger[i]
		}
	}
	return n
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
	if between(n.ID(), x.ID(), n.successors[0].ID()) {
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
	switch p := n.predecessor.(type) {
	case nil, *LocalNode:
		if n.predecessor == nil || between(n.predecessor.ID(), m.ID(), n.ID()) {
			n.predecessor = m
		}
	case *RemoteNode:
		if _, err := p.op("", ""); err == nil && between(n.predecessor.ID(), m.ID(), n.ID()) {
			n.predecessor = m
		}
	}
	// discard data up to n.predecessor.ID() asynchronously
	go n.onPredecessor(n.predecessor)
	return nil
}

func (n *LocalNode) OnPredecessor(fn func(Node)) {
	n.onPredecessor = fn
}

func (n *LocalNode) FixFingers(i int) error {
	s, err := n.FindSuccessor(n.ID() + (1 << (i % M)))
	if err != nil { // try an earlier finger.
		n.finger[(i % M)] = n.finger[(i+M-1)%M]
		return err
	}
	n.finger[(i % M)] = s
	return nil
}

func (n *LocalNode) HTTPHandlerFunc() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("op") {
		case "Successors":
			for i := 0; i < len(n.successors); i++ {
				w.Write([]byte(n.successors[i].Serialize()))
				if i != len(n.successors)-1 {
					w.Write([]byte("\n"))
				}
			}
		case "Predecessor":
			if n.predecessor == nil {
				w.WriteHeader(200)
			} else {
				w.Write([]byte(n.predecessor.Serialize()))
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
	ps := "nil"
	if n.predecessor != nil {
		ps = n.predecessor.Serialize()
	}
	ss := [R]string{}
	for i := 0; i < R; i++ {
		ss[i] = n.successors[i].Serialize()
	}
	return fmt.Sprintf("local[%s]\npredecessor: %s\nsuccessors: %s", n.Serialize(), ps, ss)
}

func (n *LocalNode) Join(ctx context.Context) {
	// start stabilization loops
	stabilize := time.NewTicker(1 * time.Second)
	fixFingers := time.NewTicker(100 * time.Millisecond)
	defer stabilize.Stop()
	defer fixFingers.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stabilize.C:
			if err := n.Stabilize(); err != nil {
				for i := 0; i < R-1; i++ {
					n.successors[i] = n.successors[i+1]
				}
			}
		case t := <-fixFingers.C:
			if err := n.FixFingers(t.Nanosecond()); err != nil {
				// TODO: this error is likely transient, can we remove it?
				log.Printf("got error %v", err)
			}
		}
	}
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
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
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
	tokens, err := n.op("FindSuccessor", fmt.Sprintf("id=%x", id))
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
