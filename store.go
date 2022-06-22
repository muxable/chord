package chord

import (
	"bytes"
	"fmt"
	"io"
	"log"
)

type Store interface {
	Set(key uint64, value io.Reader) error
	Get(key uint64) (io.Reader, error)
	All() map[uint64][]byte
	Constrain(a, b uint64) error
}

type MemoryStore map[uint64][]byte

func (s MemoryStore) Set(key uint64, value io.Reader) error {
	b, err := io.ReadAll(value)
	if err != nil {
		return err
	}
	s[key] = b;
	return nil
}

func (s MemoryStore) Get(key uint64) (io.Reader, error) {
	return bytes.NewReader(s[key]), nil
}

func (s MemoryStore) All() map[uint64][]byte {
	return s
}

func (s MemoryStore) Constrain(a, b uint64) error {
	for k := range s {
		if !between(a, k, b) {
			log.Printf("deleting %x", k)
			delete(s, k)
		}
	}
	return nil
}

func (s MemoryStore) String() string {
	out := ""
	for k, v := range s {
		out += fmt.Sprintf("%x: %v\n", k, v)
	}
	return out
}