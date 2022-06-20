package chord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

type DHTServer struct {
	node  *LocalNode
	store Store
}

// NewDHTServer binds a node to a given store.
func NewDHTServer(node *LocalNode, store Store) (*DHTServer, error) {
	node.OnPredecessor(func(predecessor Node) {
		// delete all the keys up to the predecessor's id because they now own it.
		if err := store.Constrain(predecessor.ID(), node.ID()); err != nil {
			// TODO: error handling
			log.Printf("error when constraining %v", err)
		}
	})
	// make this node a replicant of the successor.
	resp, err := http.Get(fmt.Sprintf("http://%s/store", node.successors[0].Host()))
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var data map[uint64][]byte
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	for key, value := range data {
		if err := store.Set(key, value); err != nil {
			return nil, err
		}
	}
	return &DHTServer{node: node, store: store}, nil
}

func (s *DHTServer) Get(key uint64) ([]byte, error) {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return nil, err
	}
	resp, err := http.Get(fmt.Sprintf("http://%s/store?key=%x", node.Host(), key))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

func (s *DHTServer) Set(key uint64, value []byte) error {
	node, err := s.node.FindSuccessor(key)
	if err != nil {
		return err
	}
	_, err = http.Post(fmt.Sprintf("http://%s/store?key=%x", node.Host(), key), "application/octet-stream", bytes.NewReader(value))
	return err
}

func (s *DHTServer) HTTPServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/node", s.node.HTTPHandlerFunc())
	mux.Handle("/store", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case "GET":
			key := req.URL.Query().Get("key")
			if key == "" {
				body, err := json.Marshal(s.store.All())
				if err != nil {
					w.WriteHeader(500)
					return
				}
				if _, err := w.Write(body); err != nil {
					w.WriteHeader(500)
					return
				}
			} else {
				intkey, err := strconv.ParseUint(key, 16, 64)
				if err != nil {
					w.WriteHeader(500)
					return
				}
				value, err := s.store.Get(intkey)
				if err != nil {
					w.WriteHeader(500)
					return
				}
				if _, err := w.Write(value); err != nil {
					w.WriteHeader(500)
					return
				}
			}

		case "POST":
			key := req.URL.Query().Get("key")
			body, err := io.ReadAll(req.Body)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			if key == "" {
				var data map[uint64][]byte
				if err := json.Unmarshal(body, &data); err != nil {
					w.WriteHeader(400)
					return
				}
				for key, value := range data {
					if err := s.store.Set(key, value); err != nil {
						w.WriteHeader(500)
						return
					}
				}
			} else {
				intkey, err := strconv.ParseUint(key, 16, 64)
				if err != nil {
					w.WriteHeader(500)
					return
				}
				if err := s.store.Set(intkey, body); err != nil {
					w.WriteHeader(500)
					return
				}
			}
		default:
			w.WriteHeader(400)
		}
	}))
	return mux
}

func (s *DHTServer) Close() error {
	// send the data to the predecessor.
	body, err := json.Marshal(s.store.All())
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/store", s.node.predecessor.Host()), "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}
	return nil
}