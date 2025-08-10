#!/usr/bin/env python3
import math
import random

import simpy

from lib.common import calc_dist, find_random_position
from lib.mac import set_transmit_delay, get_retransmission_msec
from lib.phy import check_collision, is_channel_active, airtime
from lib.packet import NODENUM_BROADCAST, MeshPacket, MeshMessage


class MeshNode:
    def __init__(self, conf, nodes, env, bc_pipe, nodeid, period, messages, packetsAtN, packets, delays, nodeConfig, messageSeq, verboseprint):
        self.conf = conf
        self.nodeid = nodeid
        self.verboseprint = verboseprint
        self.moveRng = random.Random(nodeid)
        self.nodeRng = random.Random(nodeid)
        self.rebroadcastRng = random.Random()
        if nodeConfig is not None:
            self.x = nodeConfig['x']
            self.y = nodeConfig['y']
            self.z = nodeConfig['z']
            self.isRouter = nodeConfig['isRouter']
            self.isRepeater = nodeConfig['isRepeater']
            self.isClientMute = nodeConfig['isClientMute']
            self.hopLimit = nodeConfig['hopLimit']    # for clarity, hop represents the numebr of times the packet can be forwarded
            self.antennaGain = nodeConfig['antennaGain']
        else:
            self.x, self.y = find_random_position(self.conf, nodes)
            self.z = self.conf.HM
            self.isRouter = self.conf.router
            self.isRepeater = False
            self.isClientMute = False
            self.hopLimit = self.conf.hopLimit
            self.antennaGain = self.conf.GL
        self.messageSeq = messageSeq
        self.env = env
        self.period = period
        self.bc_pipe = bc_pipe
        self.rx_snr = 0
        self.nodes = nodes
        self.messages = messages
        self.packetsAtN = packetsAtN
        self.nrPacketsSent = 0
        self.packets = packets
        self.delays = delays
        self.leastReceivedHopLimit = {}
        self.routingTable = {}  # routing table for the node -> {destId: (nextHop, hopLimit)}
        self.packets_queue = []
        self.nrPacketsSent = 0
        self.isReceiving = []
        self.isTransmitting = False
        self.usefulPackets = 0
        self.txAirUtilization = 0
        self.airUtilization = 0
        self.droppedByDelay = 0
        self.rebroadcastPackets = 0
        self.isMoving = False
        self.gpsEnabled = False
        # Track last broadcast position/time
        self.lastBroadcastX = self.x
        self.lastBroadcastY = self.y
        self.lastBroadcastTime = 0
        # track total transmit time for the last 6 buckets (each is 10s in firmware logic)
        self.channelUtilization = [0] * self.conf.CHANNEL_UTILIZATION_PERIODS  # each entry is ms spent on air in that interval
        self.channelUtilizationIndex = 0  # which "bucket" is current
        self.prevTxAirUtilization = 0.0   # how much total tx air-time had been used at last sample

        env.process(self.track_channel_utilization(env))
        if not self.isRepeater:  # repeaters don't generate messages themselves
            env.process(self.generate_message())
        env.process(self.receive(self.bc_pipe.get_output_conn()))
        self.transmitter = simpy.Resource(env, 1)

        # start mobility if enabled
        if self.conf.MOVEMENT_ENABLED and self.moveRng.random() <= self.conf.APPROX_RATIO_NODES_MOVING:
            self.isMoving = True
            if self.moveRng.random() <= self.conf.APPROX_RATIO_OF_NODES_MOVING_W_GPS_ENABLED:
                self.gpsEnabled = True

            # Randomly assign a movement speed
            possibleSpeeds = [
                self.conf.WALKING_METERS_PER_MIN,  # e.g.,  96 m/min
                self.conf.BIKING_METERS_PER_MIN,   # e.g., 390 m/min
                self.conf.DRIVING_METERS_PER_MIN   # e.g., 1500 m/min
            ]
            self.movementStepSize = self.moveRng.choice(possibleSpeeds)

            env.process(self.move_node(env))

    # function to dyniamically update the next node in the routing table
    def updateRoutingTable(self, origin, sender, hopCount):
        if origin == self.nodeid:
            # If the origin is this node, we don't update the routing table
            return
        if origin not in self.routingTable:
            self.routingTable[origin] = {
                "nextHop": sender,
                "cost": hopCount
            }
        else:
            if hopCount < self.routingTable[origin]["cost"]:
                # If the new hop count is lower, update the routing table
                self.routingTable[origin] = {
                    "nextHop": sender,
                    "cost": hopCount
                }

    # Simple congestion metric based on the number of packets in the queue
    def get_congestion(self):
        """Return a simple congestion metric"""
        return len(self.packets_queue) 
    
    # Choose next hop based on congestion
    def choose_next_hop_with_congestion(self, destId):
        """Decide next hop based on congestion"""
        route = self.routingTable.get(destId)
        if route:
            next_hop = route["nextHop"]
            # find the neighbor node with the next hop
            neighbor_node = next((n for n in self.nodes if n.nodeid == next_hop), None)
            if neighbor_node:           # if the neighbor node exists
                congestion = neighbor_node.get_congestion()
                if congestion < self.conf.MAX_CONGESTION_THRESHOLD:
                    self.verboseprint(f"[CC] Node {self.nodeid}: {next_hop} congestion={congestion} → OK")
                    return next_hop
                else:   # if congestion is too high, return None to indicate fallback
                    self.verboseprint(f"[CC] Node {self.nodeid}: {next_hop} congestion={congestion} → TOO HIGH, fallback")
                    return None
        self.verboseprint(f"[CC] Node {self.nodeid}: No route found → fallback")
        return None

    def track_channel_utilization(self, env):
        """
        Periodically compute how many seconds of airtime this node consumed
        over the last 10-second block and store it in the ring buffer.
        """
        while True:
            # Wait 10 seconds of simulated time
            yield env.timeout(self.conf.TEN_SECONDS_INTERVAL)

            curTotalAirtime = self.txAirUtilization  # total so far, in *milliseconds*
            blockAirtimeMs = curTotalAirtime - self.prevTxAirUtilization

            self.channelUtilization[self.channelUtilizationIndex] = blockAirtimeMs

            self.prevTxAirUtilization = curTotalAirtime
            self.channelUtilizationIndex = (self.channelUtilizationIndex + 1) % self.conf.CHANNEL_UTILIZATION_PERIODS

    def channel_utilization_percent(self) -> float:
        """
        Returns how much of the last 60 seconds (6 x 10s) this node spent transmitting, as a percent.
        """
        sumMs = sum(self.channelUtilization)
        # 6 intervals, each 10 seconds = 60,000 ms total
        # fraction = sum_ms / 60000, then multiply by 100 for percent
        return (sumMs / (self.conf.CHANNEL_UTILIZATION_PERIODS * self.conf.TEN_SECONDS_INTERVAL)) * 100.0

    def move_node(self, env):
        while True:

            # Pick a random direction and distance
            angle = 2 * math.pi * self.moveRng.random()
            distance = self.movementStepSize * self.moveRng.random()

            # Compute new position
            dx = distance * math.cos(angle)
            dy = distance * math.sin(angle)

            leftBound = self.conf.OX - self.conf.XSIZE / 2
            rightBound = self.conf.OX + self.conf.XSIZE / 2
            bottomBound = self.conf.OY - self.conf.YSIZE / 2
            topBound = self.conf.OY + self.conf.YSIZE / 2

            # Then in moveNode:
            new_x = min(max(self.x + dx, leftBound), rightBound)
            new_y = min(max(self.y + dy, bottomBound), topBound)

            # Update node’s position
            self.x = new_x
            self.y = new_y

            if self.gpsEnabled:
                distanceTraveled = calc_dist(self.lastBroadcastX, self.x, self.lastBroadcastY, self.y)
                timeElapsed = env.now - self.lastBroadcastTime
                if distanceTraveled >= self.conf.SMART_POSITION_DISTANCE_THRESHOLD and timeElapsed >= self.conf.SMART_POSITION_DISTANCE_MIN_TIME:
                    currentUtil = self.channel_utilization_percent()
                    if currentUtil < 25.0:
                        self.send_packet(NODENUM_BROADCAST, "POSITION")
                        self.lastBroadcastX = self.x
                        self.lastBroadcastY = self.y
                        self.lastBroadcastTime = env.now
                    else:
                        self.verboseprint(f"At time {env.now} node {self.nodeid} SKIPS POSITION broadcast (util={currentUtil:.1f}% > 25%)")

            # Wait until next move
            nextMove = self.get_next_time(self.conf.ONE_MIN_INTERVAL)
            if nextMove >= 0:
                yield env.timeout(nextMove)
            else:
                break

    def send_packet(self, destId, type=""):
        # increment the shared counter
        self.messageSeq["val"] += 1
        messageSeq = self.messageSeq["val"]
        self.messages.append(MeshMessage(self.nodeid, destId, self.env.now, messageSeq))
        p = MeshPacket(self.conf, self.nodes, self.nodeid, destId, self.nodeid, self.conf.PACKETLENGTH, messageSeq, self.env.now, True, False, None, self.env.now, self.verboseprint)
        self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'generated', type, 'message', p.seq, 'to', destId)
        self.packets.append(p)
        self.env.process(self.transmit(p))
        return p

    def get_next_time(self, period):
        nextGen = self.nodeRng.expovariate(1.0 / float(period))
        # do not generate message near the end of the simulation (otherwise flooding cannot finish in time)
        if self.env.now+nextGen + self.hopLimit * airtime(self.conf, self.conf.SFMODEM[self.conf.MODEM], self.conf.CRMODEM[self.conf.MODEM], self.conf.PACKETLENGTH, self.conf.BWMODEM[self.conf.MODEM]) < self.conf.SIMTIME:
            return nextGen
        return -1

    def generate_message(self):
        while True:
            # Returns -1 if we don't make it before the sim ends
            nextGen = self.get_next_time(self.period)
            # do not generate a message near the end of the simulation (otherwise flooding cannot finish in time)
            if nextGen >= 0:
                yield self.env.timeout(nextGen)

                if self.conf.DMs:
                    destId = self.nodeRng.choice([i for i in range(0, len(self.nodes)) if i is not self.nodeid])
                else:
                    destId = NODENUM_BROADCAST

                p = self.send_packet(destId)

                while p.wantAck:  # ReliableRouter: retransmit message if no ACK received after timeout
                    retransmissionMsec = get_retransmission_msec(self, p)
                    yield self.env.timeout(retransmissionMsec)

                    ackReceived = False  # check whether you received an ACK on the transmitted message
                    minRetransmissions = self.conf.maxRetransmission
                    for packetSent in self.packets:
                        if packetSent.origTxNodeId == self.nodeid and packetSent.seq == p.seq:
                            if packetSent.retransmissions < minRetransmissions:
                                minRetransmissions = packetSent.retransmissions
                            if packetSent.ackReceived:
                                ackReceived = True
                    if ackReceived:
                        self.verboseprint('Node', self.nodeid, 'received ACK on generated message with seq. nr.', p.seq)
                        break
                    else:
                        if minRetransmissions > 0:  # generate new packet with same sequence number
                            pNew = MeshPacket(self.conf, self.nodes, self.nodeid, p.destId, self.nodeid, p.packetLen, p.seq, p.genTime, p.wantAck, False, None, self.env.now, self.verboseprint)
                            pNew.retransmissions = minRetransmissions - 1
                            self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'wants to retransmit its generated packet to', destId, 'with seq.nr.', p.seq, 'minRetransmissions', minRetransmissions)
                            self.packets.append(pNew)
                            self.env.process(self.transmit(pNew))
                        else:
                            self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'reliable send of', p.seq, 'failed.')
                            break
            else:  # do not send this message anymore, since it is close to the end of the simulation
                break

    # Transmit a packet, with listen-before-talk and collision detection
    def transmit(self, packet):
        with self.transmitter.request() as request:
            yield request

            # Listen-Before-Talk (backoff)
            txTime = set_transmit_delay(self, packet)
            self.verboseprint(f"[TX] t={round(self.env.now,3)} node={self.nodeid} backoff={txTime}")
            yield self.env.timeout(txTime)

            # Wait if channel or node is currently busy
            while any(self.isReceiving) or self.isTransmitting or is_channel_active(self, self.env):
                txTime = set_transmit_delay(self, packet)
                self.verboseprint(f"[TX] t={round(self.env.now,3)} node={self.nodeid} busy → wait {txTime}")
                yield self.env.timeout(txTime)

            # ACK / TTL bookkeeping
            if packet.seq not in self.leastReceivedHopLimit:
                self.leastReceivedHopLimit[packet.seq] = packet.hopLimit + 1
            if self.leastReceivedHopLimit[packet.seq] <= packet.hopLimit:
                # Already ACKed / better copy received → abort this tx
                self.verboseprint(f"[TX] t={round(self.env.now,3)} node={self.nodeid} abort seq={packet.seq} (acked earlier)")

                # SAFE removal (packet may have been removed elsewhere)
                if packet in self.packets:
                    self.packets.remove(packet)
                else:
                    self.verboseprint(f"[TX] node={self.nodeid} seq={packet.seq} not in self.packets; skip removal")

                return

            self.verboseprint(f"[TX] t={round(self.env.now,3)} node={self.nodeid} send seq={packet.seq} hopLimit={packet.hopLimit}")
            self.nrPacketsSent += 1

            # ---------- Target selection ----------
            def flood_targets():
                """Return all nodes that can hear this transmission"""
                return [n.nodeid for n in self.nodes if packet.sensedByN[n.nodeid]]

            targets = flood_targets()  # Default: flood
            if self.conf.SELECTED_ROUTER_TYPE == self.conf.ROUTER_TYPE.MANAGED_ROUTING_TABLE and self.routingTable is not None:
                route = self.routingTable.get(packet.destId)
                if route:
                    nh = route.get("nextHop")
                    if nh is not None and packet.sensedByN[nh]:
                        targets = [nh]  # change target to next hop only
                        self.verboseprint(f"[TX] node={self.nodeid} unicast→{nh} (dest={packet.destId})")
                    else:
                        self.verboseprint(f"[TX] node={self.nodeid} no valid nextHop → flood")
                else:
                    self.verboseprint(f"[TX] node={self.nodeid} no route for dest={packet.destId} → flood")
            else:
                self.verboseprint(f"[TX] node={self.nodeid} router=FLOOD → flood")

            # ---------- Physical transmission ----------
            for rx_id in targets:
                if check_collision(self.conf, self.env, packet, rx_id, self.packetsAtN) == 0:
                    self.packetsAtN[rx_id].append(packet)

            # Timing and air utilization stats
            packet.startTime = self.env.now
            packet.endTime = self.env.now + packet.timeOnAir
            self.txAirUtilization += packet.timeOnAir
            self.airUtilization += packet.timeOnAir
            self.bc_pipe.put(packet)
            self.isTransmitting = True
            yield self.env.timeout(packet.timeOnAir)
            self.isTransmitting = False
    
    def smart_forward(self, packet):
        """Forward packet using routing table + congestion check"""
        if packet.hopLimit <= 0:        # if hop limit expired, do not forward
            self.verboseprint(f"[SF] Node {self.nodeid}: hop limit expired for packet {packet.seq}")
            return

        pNew = MeshPacket(
            self.conf,
            self.nodes,
            packet.origTxNodeId,      # origin stays the same
            packet.destId,            # destination stays the same
            self.nodeid,              # this node is now the transmitter
            packet.packetLen,         # same payload length
            packet.seq,               # same message sequence
            packet.genTime,           # original generation time (for end-to-end delay stats)
            packet.wantAck,           # preserve reliability requirement
            False,               # isAck=False (this is a data forward)
            None,                
            self.env.now,        
            self.verboseprint
            )

        pNew.hopLimit -= 1

        next_hop = self.choose_next_hop_with_congestion(pNew.destId)
        if next_hop is not None:
            self.verboseprint(f"[SF] Node {self.nodeid}: Routing packet {packet.seq} → {next_hop}")
            self.packets_queue.append(pNew)
            self.env.process(self.transmit(pNew, specific_receiver=next_hop))
        else:
            self.verboseprint(f"[SF] Node {self.nodeid}: Flooding packet {packet.seq}")
            self.packets_queue.append(pNew)
            self.env.process(self.transmit(pNew))


    def receive(self, in_pipe):
        while True:
            p = yield in_pipe.get()
            if p.sensedByN[self.nodeid] and not p.collidedAtN[self.nodeid] and p.onAirToN[self.nodeid]:  # start of reception
                if not self.isTransmitting:
                    self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'started receiving packet', p.seq, 'from', p.txNodeId)
                    p.onAirToN[self.nodeid] = False
                    self.isReceiving.append(True)
                else:  # if you were currently transmitting, you could not have sensed it
                    self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'was transmitting, so could not receive packet', p.seq)
                    p.sensedByN[self.nodeid] = False
                    p.onAirToN[self.nodeid] = False
            elif p.sensedByN[self.nodeid]:  # end of reception
                try:
                    self.isReceiving[self.isReceiving.index(True)] = False
                except Exception:
                    pass
                self.airUtilization += p.timeOnAir
                if p.collidedAtN[self.nodeid]:
                    self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'could not decode packet.')
                    continue
                p.receivedAtN[self.nodeid] = True
                self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'received packet', p.seq, 'with delay', round(self.env.now - p.genTime, 2))
                self.delays.append(self.env.now - p.genTime)
                self.updateRoutingTable(p.origTxNodeId, p.txNodeId, self.hopLimit - p.hopLimit + 1)


                # update hopLimit for this message
                if p.seq not in self.leastReceivedHopLimit:  # did not yet receive packet with this seq nr.
                    # self.verboseprint('Node', self.nodeid, 'received packet nr.', p.seq, 'orig. Tx', p.origTxNodeId, "for the first time.")
                    self.usefulPackets += 1
                    self.leastReceivedHopLimit[p.seq] = p.hopLimit
                if p.hopLimit < self.leastReceivedHopLimit[p.seq]:  # hop limit of received packet is lower than previously received one
                    self.leastReceivedHopLimit[p.seq] = p.hopLimit

                # check if implicit ACK for own generated message
                if p.origTxNodeId == self.nodeid:
                    if p.isAck:
                        self.verboseprint('Node', self.nodeid, 'received real ACK on generated message.')
                    else:
                        self.verboseprint('Node', self.nodeid, 'received implicit ACK on message sent.')
                    p.ackReceived = True
                    continue

                ackReceived = False
                realAckReceived = False
                for sentPacket in self.packets:
                    # check if ACK for message you currently have in queue
                    if sentPacket.txNodeId == self.nodeid and sentPacket.seq == p.seq:
                        self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'received implicit ACK for message in queue.')
                        ackReceived = True
                        sentPacket.ackReceived = True
                    # check if real ACK for message sent
                    if sentPacket.origTxNodeId == self.nodeid and p.isAck and sentPacket.seq == p.requestId:
                        self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'received real ACK.')
                        realAckReceived = True
                        sentPacket.ackReceived = True

                # send real ACK if you are the destination and you did not yet send the ACK
                if p.wantAck and p.destId == self.nodeid and not any(pA.requestId == p.seq for pA in self.packets):
                    self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'sends a flooding ACK.')
                    self.messageSeq["val"] += 1
                    messageSeq = self.messageSeq["val"]
                    self.messages.append(MeshMessage(self.nodeid, p.origTxNodeId, self.env.now, messageSeq))
                    pAck = MeshPacket(self.conf, self.nodes, self.nodeid, p.origTxNodeId, self.nodeid, self.conf.ACKLENGTH, messageSeq, self.env.now, False, True, p.seq, self.env.now, self.verboseprint)
                    self.packets.append(pAck)
                    self.env.process(self.transmit(pAck))
                # Rebroadcasting Logic for received message. This is a broadcast or a DM not meant for us.
                elif not p.destId == self.nodeid and not ackReceived and not realAckReceived and p.hopLimit > 0:
                    # FloodingRouter: rebroadcast received packet
                    if self.conf.SELECTED_ROUTER_TYPE == self.conf.ROUTER_TYPE.MANAGED_FLOOD:
                        if not self.isClientMute:
                            #print("FLODDING!!!!!!") # debug
                            self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'rebroadcasts received packet', p.seq)
                            pNew = MeshPacket(self.conf, self.nodes, p.origTxNodeId, p.destId, self.nodeid, p.packetLen, p.seq, p.genTime, p.wantAck, False, None, self.env.now, self.verboseprint)
                            pNew.hopLimit = p.hopLimit - 1  
                            self.packets.append(pNew)
                            self.env.process(self.transmit(pNew))   # transmit rebroadcasted packet
                    # Transit packet with routing table method
                    elif self.conf.SELECTED_ROUTER_TYPE == self.conf.ROUTER_TYPE.MANAGED_ROUTING_TABLE:
                        if not self.isClientMute:
                            #print("here") # debug
                            self.verboseprint('At time', round(self.env.now, 3), 'node', self.nodeid, 'smart-forwards received packet', p.seq)
                            self.smart_forward(p)
                else:
                    self.droppedByDelay += 1
