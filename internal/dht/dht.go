package dht

type DHT struct {
	Self         Node
	RoutingTable *RoutingTable
	Providers    *ProviderStore
}

func NewDHT(self Node) *DHT {
	return &DHT{
		Self:         self,
		RoutingTable: NewRoutingTable(self.ID),
		Providers:    NewStore(),
	}
}

func (d *DHT) AddPeer(peer Node) {
	if d == nil {
		return
	}
	if d.RoutingTable == nil {
		d.RoutingTable = NewRoutingTable(d.Self.ID)
	}

	d.RoutingTable.AddNode(peer)
}

func (d *DHT) RecordProvider(chunkHash string, providerID string) {
	if d == nil {
		return
	}
	if d.Providers == nil {
		d.Providers = NewStore()
	}

	d.Providers.Add(chunkHash, providerID)
}

// ProvidersFor returns the provider IDs known locally for a chunk hash.
func (d *DHT) ProvidersFor(chunkHash string) []string {
	if d == nil {
		return nil
	}
	if d.Providers == nil {
		return nil
	}

	return d.Providers.Get(chunkHash)
}

// ClosestPeers returns the nearest known peers to a target ID.
func (d *DHT) ClosestPeers(targetID string, count int) []Node {
	if d == nil || d.RoutingTable == nil {
		return nil
	}

	return d.RoutingTable.ClosestNodes(targetID, count)
}

func (d *DHT) HandleMessage(message Message) Message {
	if d == nil {
		return Message{}
	}
	if d.Providers == nil {
		d.Providers = NewStore()
	}
	if d.RoutingTable == nil {
		d.RoutingTable = NewRoutingTable(d.Self.ID)
	}

	response := Message{
		Type: message.Type,
		From: d.Self,
		To:   message.From,
	}

	switch message.Type {
	case MsgPing:
		response.Type = MsgPong
	case MsgFindNode:
		response.Nodes = d.ClosestPeers(message.TargetID, K)
	case MsgFindValue:
		response.Nodes = d.ClosestPeers(message.TargetID, K)
		response.Providers = d.ProvidersFor(message.Key)
	case MsgStore:
		if message.Key != "" && message.From.ID != "" {
			d.RecordProvider(message.Key, message.From.ID)
		}
	}

	return response
}
