package dht

//message represents an RPC message exchanged between peers.

type Message struct {
	Type      string
	From      Node
	To        Node
	TargetID  string
	Key       string
	Value     []byte
	Nodes     []Node
	Providers []string
	Error     string
}
