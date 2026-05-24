package dht

//lookup functions for finding nodes and values.

func FindClosest(targetID string, table *RoutingTable, count int) []Node {
	if table == nil {
		return nil
	}

	return table.ClosestNodes(targetID, count)
}
