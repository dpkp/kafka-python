from __future__ import absolute_import, division, print_function


class GroupState:
    UNJOINED = '<unjoined>'
    JOINING = '<joining>'
    SYNCING = '<syncing>'
    STABLE = '<stable>'
    NO_BROKER = '<no-broker>'


class GroupInputs:
    BEGIN_JOIN = '<begin-join>'
    JOINED_LEADER = '<joined-leader>'
    JOINED_FOLLOWER = '<joined-follower>'
    SYNCED = '<synced>'
    UNKNOWN_MEMBER = '<unknown-member>'
    ILLEGAL_GENERATION = '<illegal-generation>'
    REBALANCE_IN_PROGRESS = '<rebalance-in-progress>'
    CLOSE = '<close>'
    #NOT_COORDINATOR_ERROR = '<not-coordinator-error>'
    #UNKNOWN_ERROR = '<unknown-error>'
    #AUTHORIZATION_ERROR = '<authorization-error>'
    #HEARTBEAT_ERROR = '<heartbeat-error>'


class GroupOutputs:
    SEND_INIT_JOIN = '<send-init-join>'
    SEND_REJOIN = '<send-rejoin>'
    SEND_SYNC_LEADER = '<send-sync-leader>'
    SEND_SYNC_FOLLOWER = '<send-sync-follower>'
    SEND_LEAVE_GROUP = '<send-leave-group>'
    FATAL_ERROR = '<fatal-error>'

"""
class RequestBuilder:
    INIT_JOIN -> (group_id, session_timeout_ms, max_poll_interval_ms, protocol_type, [(assignor, metadata), ...])
    REJOIN -> (group_id, session_timeout_ms, max_poll_interval_ms, member_id, protocol_type, [(assignor, metadata), ...])
    SYNC_LEADER -> (group_id, generation_id, member_id, leader_id, group_protocol, group_assignment) [(member_id, metadata), ...], 
    SYNC_FOLLOWER -> (group_id, generation_id, member_id)

    group_id
    session_timeout_ms
    max_poll_interval_ms
    protocol_type
"""


class KafkaGroupStateMachine:
    # State -> Input -> (New State, Output)
    TRANSITIONS = {
        GroupState.UNJOINED: {
            GroupInputs.BEGIN_JOIN:      (GroupState.JOINING, GroupOutputs.SEND_INIT_JOIN),
        }
        GroupState.JOINING: {
            GroupInputs.JOINED_LEADER:   (GroupState.SYNCING, GroupOutputs.SEND_SYNC_LEADER),
            GroupInputs.JOINED_FOLLOWER: (GroupState.SYNCING, GroupOutputs.SEND_SYNC_FOLLOWER),
            GroupInputs.UNKNOWN_MEMBER:  (GroupState.JOINING, GroupOutputs.SEND_INIT_JOIN),
        }
        GroupState.SYNCING: {
            GroupInputs.SYNCED:                (GroupState.STABLE, None),
            GroupInputs.UNKNOWN_MEMBER:        (GroupState.JOINING, GroupOutputs.SEND_INIT_JOIN),
            GroupInputs.ILLEGAL_GENERATION:    (GroupState.JOINING, GroupOutputs.SEND_REJOIN),
            GroupInputs.REBALANCE_IN_PROGRESS: (GroupState.JOINING, GroupOutputs.SEND_REJOIN),
        }
        GroupState.STABLE: {
            GroupInputs.BEGIN_JOIN:            (GroupState.JOINING, GroupOutputs.SEND_REJOIN),
            GroupInputs.HEARTBEAT:             (GroupState.STABLE, None),
            GroupInputs.CLOSE:                 (GroupState.UNJOINED, GroupOutputs.SEND_LEAVE_GROUP),
            GroupInputs.UNKNOWN_MEMBER:        (GroupState.JOINING, GroupOutputs.SEND_INIT_JOIN),
            GroupInputs.ILLEGAL_GENERATION:    (GroupState.JOINING, GroupOutputs.SEND_REJOIN),
            GroupInputs.REBALANCE_IN_PROGRESS: (GroupState.JOINING, GroupOutputs.SEND_REJOIN),
        }
    }

    def __init__(self):
        self.state = GroupState.UNJOINED
        self.group_id = group_id
        self.member_id = ''
        self.session_timeout_ms = session_timeout_ms
        self.protocol_type = protocol_type
        self.heartbeat_interval = 0.5
        self.last_heartbeat = -1

    def process_input(self, input_):
        """Given a state and an input, return a new state and any triggered output"""
        try:
            new_state, output = self.TRANSITIONS[self.state][input_]
        except KeyError:
            raise ValueError('Unrecognized state/input')
        else:
            self.state = new_state
            if input_ is GroupState.HEARTBEAT:
                self.last_heartbeat = time.time()
            return output

    def should_heartbeat(self):
        if self.state is not GroupState.STABLE:
            return False
        if self.last_heartbeat > time.time() + self.heartbeat_interval:
            return False
        return True

    def join(self, protocols):
        # protocols: list of (protocol, metadata) tuples
        # but this is only needed in connection, not FSM
        pass

    def assign(self, group_assignment):
        pass

    def leave(self):
        pass


class Coordinator:
    def __init__(self, protocol, fsm):
        self.protocol = protocol
        self.fsm = fsm
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def receive_bytes(self, data):
        responses = self.protocol.receive_bytes(data)
        for _, response in responses:
            event = self.response_to_event(response)
            self.receive_event(event)

    def receive_event(self, event):
        """Take api responses, update state, return Requests"""
        # would an Event class work better here?
        # Could be ApiResponseEvent, LocalEvent (perform assignment, start heartbeat)
        output_events = self.fsm.process_input(event)
        for output_event in output_events:
            self.process_event(output_event)

    def process_event(self, event):
        if event == GroupOutputs.SEND_INIT_JOIN:
            self.send_init_join()
        elif event == GroupOutputs.SEND_REJOIN:
            self.send_rejoin()
        elif event == GroupOutputs.SEND_SYNC_LEADER:
            self.send_sync_leader()
        elif event == GroupOutputs.SEND_SYNC_FOLLOWER:
            self.send_sync_follower()
        elif event == GroupOutputs.START_HEARTBEAT:
            self.start_heartbeats()
        elif event == GroupOutputs.STOP_HEARTBEAT:
            self.stop_heartbeats()
        elif event == GroupOutputs.SEND_LEAVE_GROUP:
            self.send_leave()
        elif event == GroupOutput.FATAL_ERROR:
            raise Exception(event)

    def join(self):
        """Return api request"""
        self.receive_event(GroupInput.BEGIN_JOIN)

    def rejoin():
        """Returns api request"""
        # call this for offsetcommitresponse?
        pass

    def leave():
        """Leave Group"""
        pass

    def cb_do_assignment():
        pass

    def cb_start_heartbeat():
        pass

    def cb_stop_heartbeat():
        pass

    def offset_commits?
        pass

class CoordinatorGroup:
    def __init__(self, group_id, session_timeout_ms=10000, protocol_type='consumer'):
        self.group_id = group_id
        self.member_id = b''
        self.generation = None
        self.session_timeout_ms = session_timeout_ms
        self.protocol_type = protocol_type

    def send_init_join(self):
        return JoinGroupRequest[0](
            self.group_id,
            self.session_timeout_ms,
            self.member_id,
            self.protocol_type,
            [(protocol,
              metadata if isinstance(metadata, bytes) else metadata.encode())
             for protocol, metadata in self.group_protocols()])

    def send_rejoin(self):
        return JoinGroupRequest[0](
            self.group_id,
            self.session_timeout_ms,
            self.member_id,
            self.protocol_type,
            [(protocol,
              metadata if isinstance(metadata, bytes) else metadata.encode())
             for protocol, metadata in self.group_protocols()])

    def send_sync_leader(self):
        assignments = self.cb_do_assignment()
        return SyncGroupRequest[0](...)

    def send_sync_follower(self):
        return SyncGroupRequest[0](...)

    def send_heartbeat(self):
        return HeartbeatRequest[0](...)


coordinator
coordinator-transport: manages socket connection, passes received_bytes to protocol
coordinator-protocol:
    events in, events out?

    public methods (join, leave, heartbeat) pass input_events to coordinator-FSM, return nothing?

    passes received_bytes to kafka-protocol,
    passes received_events to event_to_input
    passes input_event to coordinator-FSM
    convert output_events to dispatch functions + call

    dispatch functions generate api events
    api events passed to kafka_protocol
    bytes_to_send passed to transport

coordinator-group: manages group settings, member state, generation, creates api requests
coordinator-fsm
