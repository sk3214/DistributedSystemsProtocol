package chandy_lamport

import "log"

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	//used to check if server is marked for a snapshotId
	isSnapshotOngoing *SyncMap //int and bool
	//Used to track the state of server for the particular snapshotId
	serverState *SyncMap //[int]SnapshotState
	//Used to track if the marker coming from a particular channel has already been visited or not.
	incomingChannelsMap *SyncMap //[int]map[string]bool
	//Used to track the snapshot message
	messageinChannel *SyncMap //[int]*SnapshotMessage[]
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		NewSyncMap(),
		NewSyncMap(),
		NewSyncMap(),
		NewSyncMap(),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	// Message can be of type marker or snapshot. Will have to handle these cases separately.
	switch msg := message.(type) {
	case TokenMessage:
		// In case of token message tokens will be exchanged between two servers
		server.Tokens += msg.numTokens
		server.isSnapshotOngoing.Range(func(snapshotId, isOngoing interface{}) bool {
			if isOngoing.(bool) {
				inboundMap_, _ := server.incomingChannelsMap.Load(snapshotId)
				inboundMap := inboundMap_.(map[string]bool)
				if !inboundMap[src] {
					channelMessage := SnapshotMessage{src, server.Id, TokenMessage{msg.numTokens}}
					a_, ok := server.messageinChannel.Load(snapshotId)
					if !ok {
						var channelMess []*SnapshotMessage
						channelMess = append(channelMess, &channelMessage)
						server.messageinChannel.Store(snapshotId, channelMess)
					} else {
						a := a_.([]*SnapshotMessage)
						a = append(a, &channelMessage) // TODO check pointer
						server.messageinChannel.Store(snapshotId, a)
					}
				}
			}
			return true
		})

	case MarkerMessage:
		// In case of marker message the previous marker should not hold the snapshotId and pass it along to the neighbors
		// Incoming marker.Start snapshot and send to everyone

		_, ok := server.isSnapshotOngoing.Load(msg.snapshotId)
		if !ok {
			server.StartSnapshot(msg.snapshotId)
		}

		inboundMap_, ok := server.incomingChannelsMap.Load(msg.snapshotId)
		if ok {
			inboundMap := inboundMap_.(map[string]bool)
			inboundMap[src] = true

			res := server.checkifAllincomingChannelsMarked(msg.snapshotId)
			if res {
				server.isSnapshotOngoing.Store(msg.snapshotId, false)
				server.sim.NotifySnapshotComplete(server.Id, msg.snapshotId)
			}
		}
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	// If the server has not been visited
	// 1) Record its own state using snapshotstate
	// 2) Send the marker to all outgoing channels
	// 3) Do not hold the marker
	// else
	// 1) Track the state of the all the incoming channels if the marker hasn't been received from that channel

	tokenMap := make(map[string]int)
	tokenMap[server.Id] = server.Tokens
	var messages []*SnapshotMessage
	serverState := SnapshotState{snapshotId, tokenMap, messages}
	server.serverState.Store(snapshotId, serverState)
	server.isSnapshotOngoing.Store(snapshotId, true)
	if server.checkifAllincomingChannelsMarked(snapshotId) {
		server.sim.NotifySnapshotComplete(server.Id, snapshotId)
	} else {
		server.SendToNeighbors(MarkerMessage{snapshotId})
	}
}

func (server *Server) checkifAllincomingChannelsMarked(snapshotId int) bool {
	mapForincomingChannels := make(map[string]bool)
	mapForincomingChannels[server.Id] = false
	inboundMap_, _ := server.incomingChannelsMap.LoadOrStore(snapshotId, mapForincomingChannels)
	inboundMap := inboundMap_.(map[string]bool)
	incomingChannelKeys := getSortedKeys(server.inboundLinks)
	for _, src := range incomingChannelKeys {

		if !inboundMap[src] {
			//	// All channels not marked
			return false
		}
	}
	return true
}
