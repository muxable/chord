# muxable/chord

A minimal implemenation of the Chord distributed hash table datastructure.

The implementation is based on the [original MIT paper](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf) and the [AT&T paper](https://arxiv.org/pdf/1502.06461.pdf) to improve resiliency.

Inter-node communication is done over HTTP for ease of debugging.

## Example

```
go run cmd/main.go -addr 127.0.0.1:5001
go run cmd/main.go -addr 127.0.0.1:5002 -join 127.0.0.1:5001
go run cmd/main.go -addr 127.0.0.1:5003 -join 127.0.0.1:5002
```

## Notable differences

- Node id's are not required to be the hash of an ip address. This allows multiple nodes to coexist on a given IP.
- For ease of implementation, we use a `uint64` instead of a `sha1.Size`.
