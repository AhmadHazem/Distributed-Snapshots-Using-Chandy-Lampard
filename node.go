package asg3

import (
	"log"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	savedMsg      []*MsgSnapshot
	savedtokens   map[int]int
	finished      map[int]bool
	//bufferedPackets []Message

	// TODO: add more fields here (what does each node need to keep track of?)

}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),
		savedMsg:      make([]*MsgSnapshot, 0),
		savedtokens:   make(map[int]int),
		finished:      make(map[int]bool),
		// TODO: You may need to modify this if you make modifications above

	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

func (node *Node) HandlePacket(src string, message Message) {
	if message.isMarker {
		if !node.finished[message.data] {
			node.StartSnapshot(message.data)
		} else {
			// Check if a marker was received from the same source of the saved tokens
			if savedSnapshotID, ok := node.savedtokens[message.data]; ok && savedSnapshotID == message.data {
				// Record that the tokens were in the queue during the snapshot
				for !node.inboundLinks[src].msgQueue.Empty() {
					tokenMsg := node.inboundLinks[src].msgQueue.Peek().(Message)

					node.inboundLinks[src].msgQueue.Pop()
					node.savedMsg = append(node.savedMsg, &MsgSnapshot{src, node.id, tokenMsg})
					node.tokens += tokenMsg.data
				}
			}
		}
	} else {
		node.savedMsg = append(node.savedMsg, &MsgSnapshot{src, node.id, message})
		// Save incoming tokens in the node if a snapshot is in progress
		if _, ok := node.savedtokens[message.data]; ok {
			node.inboundLinks[src].msgQueue.Push(message)
		} else {
			node.tokens += message.data
		}
	}
}

func (node *Node) StartSnapshot(snapshotId int) {
	node.savedtokens[snapshotId] = node.tokens
	marker := Message{isMarker: true, data: snapshotId}
	node.SendToNeighbors(marker)
	node.finished[snapshotId] = true

}
