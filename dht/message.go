package dht

//message represents an RPC message exchanged between peers.

type Message struct {
	Type string
	From Node
	Body []byte
}
