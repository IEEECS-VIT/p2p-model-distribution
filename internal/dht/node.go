package dht

import "strconv"

type Node struct {
	ID   string
	IP   string
	Port int
}

func (n Node) Endpoint() string {
	if n.IP == "" || n.Port == 0 {
		return ""
	}

	return n.IP + ":" + strconv.Itoa(n.Port)
}

func (n Node) IsZero() bool {
	return n.ID == "" && n.IP == "" && n.Port == 0
}
