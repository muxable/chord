package chord

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type DHTServer struct {
	node  *LocalNode
	store Store
}

// NewDHTServer binds a node to a given store.
func NewDHTServer(node *LocalNode, store Store) *DHTServer {
	return &DHTServer{node: node, store: store}
}

func (s *DHTServer) Get(key uint64) ([]byte, error) {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return nil, err
	}
	resp, err := http.Get(fmt.Sprintf("http://%s/store/%x", node.Host(), key))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

func (s *DHTServer) Put(key uint64, value []byte) error {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return err
	}
	_, err = http.Post(fmt.Sprintf("http://%s/store/%x", node.Host(), key), "application/octet-stream", bytes.NewReader(value))
	return err
}

func (s *DHTServer) Serve(lis net.Listener) error {
	mux := http.NewServeMux()
	mux.Handle("/node", s.node.HTTPHandlerFunc())
	mux.Handle("/store", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		tokens := strings.Split(req.URL.Path, "/")
		if len(tokens) < 2 {
			w.WriteHeader(404)
			return
		}
		key, err := strconv.ParseUint(tokens[2], 16, 64)
		if err != nil {
			w.WriteHeader(404)
			return
		}
		if req.Method == "GET" {
			value, err := s.store.Get(key)
			if err != nil {
				w.WriteHeader(500)
				return
			}
			w.Write(value)
		} else if req.Method == "POST" {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				w.WriteHeader(500)
				return
			}
			if err := s.store.Set(key, body); err != nil {
				w.WriteHeader(500)
			} else {
				w.Write([]byte{})
			}
		} else {
			w.WriteHeader(400)
		}
	}))
	return http.Serve(lis, mux)
}