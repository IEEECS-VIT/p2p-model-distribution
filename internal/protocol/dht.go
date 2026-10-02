package protocol

import "encoding/json"

// Extended message types beyond the protobuf-generated enum.
// These are defined here to avoid regenerating p2p.pb.go.
const (
	MSG_DHT_PING            MessageType = 7
	MSG_DHT_PONG            MessageType = 8
	MSG_DHT_FIND_NODE       MessageType = 9
	MSG_DHT_FIND_NODE_RESP  MessageType = 10
	MSG_DHT_STORE           MessageType = 11
	MSG_DHT_STORE_RESP      MessageType = 12
	MSG_DHT_FIND_VALUE      MessageType = 13
	MSG_DHT_FIND_VALUE_RESP MessageType = 14
)

// DHTNode is the wire-format representation of a DHT peer.
type DHTNode struct {
	ID   string `json:"id"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// DHTMessageBody is the JSON-serialisable payload carried inside an Envelope
// for DHT RPCs.  JSON is used instead of a separate protobuf message so we
// don't need protoc to extend the schema.
type DHTMessageBody struct {
	Type      string    `json:"type"`                // mirrors dht.Message.Type
	FromID    string    `json:"from_id"`             // sending peer's node ID
	FromIP    string    `json:"from_ip,omitempty"`   // sending peer's listener IP
	FromPort  int       `json:"from_port,omitempty"` // sending peer's listener Port
	TargetID  string    `json:"target_id,omitempty"` // for FIND_NODE / FIND_VALUE
	Key       string    `json:"key,omitempty"`       // content-hash for STORE / FIND_VALUE
	Value     []byte    `json:"value,omitempty"`
	Nodes     []DHTNode `json:"nodes,omitempty"`
	Providers []string  `json:"providers,omitempty"`
	Error     string    `json:"error,omitempty"`
}

func MarshalDHTMessage(msg *DHTMessageBody) ([]byte, error) {
	return json.Marshal(msg)
}

func UnmarshalDHTMessage(data []byte) (*DHTMessageBody, error) {
	var msg DHTMessageBody
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// RequestMessageType returns the corresponding MessageType for a DHT request.
func RequestMessageType(dhtType string) MessageType {
	switch dhtType {
	case "PING":
		return MSG_DHT_PING
	case "FIND_NODE":
		return MSG_DHT_FIND_NODE
	case "FIND_VALUE":
		return MSG_DHT_FIND_VALUE
	case "STORE":
		return MSG_DHT_STORE
	default:
		return MSG_DHT_PING
	}
}

// ResponseMessageType returns the corresponding MessageType for a DHT response.
func ResponseMessageType(dhtType string) MessageType {
	switch dhtType {
	case "PING":
		return MSG_DHT_PONG
	case "FIND_NODE":
		return MSG_DHT_FIND_NODE_RESP
	case "FIND_VALUE":
		return MSG_DHT_FIND_VALUE_RESP
	case "STORE":
		return MSG_DHT_STORE_RESP
	default:
		return MSG_DHT_PONG
	}
}
