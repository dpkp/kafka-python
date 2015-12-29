import abc
import logging
import time

import six

import kafka.common as Errors
from kafka.future import Future
from kafka.protocol.commit import (GroupCoordinatorRequest,
                                   OffsetCommitRequest_v2 as OffsetCommitRequest)
from kafka.protocol.group import (HeartbeatRequest, JoinGroupRequest,
                                  LeaveGroupRequest, SyncGroupRequest)
from .heartbeat import Heartbeat

log = logging.getLogger(__name__)


class AbstractCoordinator(object):
    """
    AbstractCoordinator implements group management for a single group member
    by interacting with a designated Kafka broker (the coordinator). Group
    semantics are provided by extending this class.  See ConsumerCoordinator
    for example usage.

    From a high level, Kafka's group management protocol consists of the
    following sequence of actions:

    1. Group Registration: Group members register with the coordinator providing
       their own metadata (such as the set of topics they are interested in).

    2. Group/Leader Selection: The coordinator select the members of the group
       and chooses one member as the leader.

    3. State Assignment: The leader collects the metadata from all the members
       of the group and assigns state.

    4. Group Stabilization: Each member receives the state assigned by the
       leader and begins processing.

    To leverage this protocol, an implementation must define the format of
    metadata provided by each member for group registration in group_protocols()
    and the format of the state assignment provided by the leader in
    _perform_assignment() and which becomes available to members in
    _on_join_complete().
    """

    _session_timeout_ms = 30000
    _heartbeat_interval_ms = 3000
    _retry_backoff_ms = 100

    def __init__(self, client, group_id, **kwargs):
        if not client:
            raise Errors.IllegalStateError('a client is required to use'
                                           ' Group Coordinator')
        if not group_id:
            raise Errors.IllegalStateError('a group_id is required to use'
                                           ' Group Coordinator')
        for config in ('session_timeout_ms',
                       'heartbeat_interval_ms',
                       'retry_backoff_ms'):
            if config in kwargs:
                setattr(self, '_' + config, kwargs.pop(config))

        self._client = client
        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.group_id = group_id
        self.coordinator_id = None
        self.rejoin_needed = True
        self.needs_join_prepare = True
        self.heartbeat = Heartbeat(
          session_timeout_ms=self._session_timeout_ms,
          heartbeat_interval_ms=self._heartbeat_interval_ms)
        self.heartbeat_task = HeartbeatTask(self)
        #self.sensors = GroupCoordinatorMetrics(metrics, metric_group_prefix, metric_tags)

    @abc.abstractmethod
    def protocol_type(self):
        """
        Unique identifier for the class of protocols implements
        (e.g. "consumer" or "connect").

        @return str protocol type name
        """
        pass

    @abc.abstractmethod
    def group_protocols(self):
        """Return the list of supported group protocols and metadata.

        This list is submitted by each group member via a JoinGroupRequest.
        The order of the protocols in the list indicates the preference of the
        protocol (the first entry is the most preferred). The coordinator takes
        this preference into account when selecting the generation protocol
        (generally more preferred protocols will be selected as long as all
        members support them and there is no disagreement on the preference).

        Note: metadata must be type bytes or support an encode() method

        @return [(protocol, metadata), ...]
        """
        pass

    @abc.abstractmethod
    def _on_join_prepare(self, generation, member_id):
        """Invoked prior to each group join or rejoin.

        This is typically used to perform any cleanup from the previous
        generation (such as committing offsets for the consumer)

        @param generation The previous generation or -1 if there was none
        @param member_id The identifier of this member in the previous group
                         or '' if there was none
        """
        pass

    @abc.abstractmethod
    def _perform_assignment(self, leader_id, protocol, members):
        """Perform assignment for the group.

        This is used by the leader to push state to all the members of the group
        (e.g. to push partition assignments in the case of the new consumer)

        @param leader_id: The id of the leader (which is this member)
        @param protocol: the chosen group protocol (assignment strategy)
        @param members: [(member_id, metadata_bytes)] from JoinGroupResponse.
                        metadata_bytes are associated with the chosen group
                        protocol, and the Coordinator subclass is responsible
                        for decoding metadata_bytes based on that protocol.

        @return dict of {member_id: assignment}; assignment must either be bytes
                or have an encode() method to convert to bytes
        """
        pass

    @abc.abstractmethod
    def _on_join_complete(self, generation, member_id, protocol,
                          member_assignment_bytes):
        """Invoked when a group member has successfully joined a group.

        @param generation The generation that was joined
        @param member_id The identifier for the local member in the group
        @param protocol The protocol selected by the coordinator
        @param member_assignment_bytes The protocol-encoded assignment
               propagated from the group leader. The Coordinator instance is
               responsible for decoding based on the chosen protocol.
        """
        pass

    def coordinator_unknown(self):
        """
        Check if we know who the coordinator is and we have an active connection

        Side-effect: reset coordinator_id to None if connection failed

        @return True if the coordinator is unknown
        """
        if self.coordinator_id is None:
            return True

        if self._client.connection_failed(self.coordinator_id):
            self.coordinator_dead()
            return True

        return not self._client.ready(self.coordinator_id)

    def ensure_coordinator_known(self):
        """Block until the coordinator for this group is known
        (and we have an active connection -- java client uses unsent queue).
        """
        while self.coordinator_unknown():

            # Dont look for a new coordinator node if we are just waiting
            # for connection to finish
            if self.coordinator_id is not None:
                self._client.poll()
                continue

            future = self.send_group_metadata_request()
            self._client.poll(future=future)

            if future.failed():
                if future.retriable():
                    metadata_update = self._client.cluster.request_update()
                    self._client.poll(future=metadata_update)
                else:
                    raise future.exception # pylint: disable-msg=raising-bad-type

    def need_rejoin(self):
        """
        Check whether the group should be rejoined (e.g. if metadata changes)
        @return True if it should, False otherwise
        """
        return self.rejoin_needed

    def ensure_active_group(self):
        """Ensure that the group is active (i.e. joined and synced)"""
        if not self.need_rejoin():
            return

        if self.needs_join_prepare:
            self._on_join_prepare(self.generation, self.member_id)
            self.needs_join_prepare = False

        while self.need_rejoin():
            self.ensure_coordinator_known()

            future = self.perform_group_join()
            self._client.poll(future=future)

            if future.succeeded():
                member_assignment_bytes = future.value
                self._on_join_complete(self.generation, self.member_id,
                                       self.protocol, member_assignment_bytes)
                self.needs_join_prepare = True
                self.heartbeat_task.reset()
            else:
                exception = future.exception
                if isinstance(exception, (Errors.UnknownMemberIdError,
                                          Errors.RebalanceInProgressError,
                                          Errors.IllegalGenerationError)):
                    continue
                elif not future.retriable():
                    raise exception # pylint: disable-msg=raising-bad-type
                time.sleep(self._retry_backoff_ms / 1000.0)

    def perform_group_join(self):
        """Join the group and return the assignment for the next generation.

        This function handles both JoinGroup and SyncGroup, delegating to
        _perform_assignment() if elected leader by the coordinator.

        @return Future() of the assignment returned from the group leader
        """
        if self.coordinator_unknown():
            e = Errors.GroupCoordinatorNotAvailableError(self.coordinator_id)
            return Future().failure(e)

        # send a join group request to the coordinator
        log.debug("(Re-)joining group %s", self.group_id)
        request = JoinGroupRequest(
            self.group_id,
            self._session_timeout_ms,
            self.member_id,
            self.protocol_type(),
            [(protocol,
              metadata if isinstance(metadata, bytes) else metadata.encode())
             for protocol, metadata in self.group_protocols()])

        # create the request for the coordinator
        log.debug("Issuing request (%s) to coordinator %s", request, self.coordinator_id)
        future = Future()
        _f = self._client.send(self.coordinator_id, request)
        _f.add_callback(self._handle_join_group_response, future)
        _f.add_errback(self._failed_request, future)
        return future

    def _failed_request(self, future, error):
        self.coordinator_dead()
        future.failure(error)

    def _handle_join_group_response(self, future, response):
        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            log.debug("Joined group: %s", response)
            self.member_id = response.member_id
            self.generation = response.generation_id
            self.rejoin_needed = False
            self.protocol = response.group_protocol
            #self.sensors.join_latency.record(response.requestLatencyMs())
            if response.leader_id == response.member_id:
                self.on_join_leader(response).chain(future)
            else:
                self.on_join_follower().chain(future)

        elif error_type is Errors.GroupLoadInProgressError:
            log.debug("Attempt to join group %s rejected since coordinator is"
                      " loading the group.", self.group_id)
            # backoff and retry
            future.failure(error_type(response))
        elif error_type is Errors.UnknownMemberIdError:
            # reset the member id and retry immediately
            error = error_type(self.member_id)
            self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
            log.info("Attempt to join group %s failed due to unknown member id,"
                     " resetting and retrying.", self.group_id)
            future.failure(error)
        elif error_type in (Errors.GroupCoordinatorNotAvailableError,
                            Errors.NotCoordinatorForGroupError):
            # re-discover the coordinator and retry with backoff
            self.coordinator_dead()
            log.info("Attempt to join group %s failed due to obsolete "
                     "coordinator information, retrying.", self.group_id)
            future.failure(error_type())
        elif error_type in (Errors.InconsistentGroupProtocolError,
                            Errors.InvalidSessionTimeoutError,
                            Errors.InvalidGroupIdError):
            # log the error and re-throw the exception
            error = error_type(response)
            log.error("Attempt to join group %s failed due to: %s",
                      self.group_id, error)
            future.failure(error)
        elif error_type is Errors.GroupAuthorizationFailedError:
            future.failure(error_type(self.group_id))
        else:
            # unexpected error, throw the exception
            error = error_type()
            log.error("Unexpected error in join group response: %s", error)
            future.failure(error)

    def on_join_follower(self):
        # send follower's sync group with an empty assignment
        request = SyncGroupRequest(
            self.group_id,
            self.generation,
            self.member_id,
            {})
        log.debug("Issuing follower SyncGroup (%s) to coordinator %s",
                  request, self.coordinator_id)
        return self.send_sync_group_request(request)

    def on_join_leader(self, response):
        """
        Perform leader synchronization and send back the assignment
        for the group via SyncGroupRequest
        """
        try:
            group_assignment = self._perform_assignment(response.leader_id,
                                                        response.group_protocol,
                                                        response.members)
        except Exception as e:
            raise
            return Future().failure(e)

        request = SyncGroupRequest(
            self.group_id,
            self.generation,
            self.member_id,
            [(member_id,
              assignment if isinstance(assignment, bytes) else assignment.encode())
             for member_id, assignment in six.iteritems(group_assignment)])

        log.debug("Issuing leader SyncGroup (%s) to coordinator %s",
                  request, self.coordinator_id)
        return self.send_sync_group_request(request)

    def send_sync_group_request(self, request):
        if self.coordinator_unknown():
            return Future().failure(Errors.GroupCoordinatorNotAvailableError())
        future = Future()
        _f = self._client.send(self.coordinator_id, request)
        _f.add_callback(self._handle_sync_group_response, future)
        _f.add_errback(self._failed_request, future)
        return future

    def _handle_sync_group_response(self, future, response):
        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            log.debug("Received successful sync group response for group %s: %s",
                      self.group_id, response)
            #self.sensors.syncLatency.record(response.requestLatencyMs())
            future.success(response.member_assignment)
            return

        # Always rejoin on error
        self.rejoin_needed = True
        if error_type is Errors.GroupAuthorizationFailedError:
            future.failure(error_type(self.group_id))
        elif error_type is Errors.RebalanceInProgressError:
            log.info("SyncGroup for group %s failed due to coordinator"
                     " rebalance, rejoining the group", self.group_id)
            future.failure(error_type(self.group_id))
        elif error_type in (Errors.UnknownMemberIdError,
                            Errors.IllegalGenerationError):
            error = error_type()
            log.info("SyncGroup for group %s failed due to %s,"
                     " rejoining the group", self.group_id, error)
            self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
            future.failure(error)
        elif error_type in (Errors.GroupCoordinatorNotAvailableError,
                            Errors.NotCoordinatorForGroupError):
            error = error_type()
            log.info("SyncGroup for group %s failed due to %s, will find new"
                     " coordinator and rejoin", self.group_id, error)
            self.coordinator_dead()
            future.failure(error)
        else:
            error = error_type()
            log.error("Unexpected error from SyncGroup: %s", error)
            future.failure(error)

    def send_group_metadata_request(self):
        """Discover the current coordinator for the group.

        Sends a GroupMetadata request to one of the brokers. The returned future
        should be polled to get the result of the request.

        @return future indicating the completion of the metadata request
        """
        node_id = self._client.least_loaded_node()
        if node_id is None or not self._client.ready(node_id):
            return Future().failure(Errors.NoBrokersAvailable())

        log.debug("Issuing group metadata request to broker %s", node_id)
        request = GroupCoordinatorRequest(self.group_id)
        future = Future()
        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_group_coordinator_response, future)
        _f.add_errback(self._failed_request, future)
        return future

    def _handle_group_coordinator_response(self, future, response):
        log.debug("Group metadata response %s", response)
        if not self.coordinator_unknown():
            # We already found the coordinator, so ignore the request
            log.debug("Coordinator already known -- ignoring metadata response")
            future.success(self.coordinator_id)
            return

        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            ok = self._client.cluster.add_group_coordinator(self.group_id, response)
            if not ok:
                # This could happen if coordinator metadata is different
                # than broker metadata
                future.failure(Errors.IllegalStateError())
                return

            self.coordinator_id = response.coordinator_id
            self._client.ready(self.coordinator_id)

            # start sending heartbeats only if we have a valid generation
            if self.generation > 0:
                self.heartbeat_task.reset()
            future.success(None)
        elif error_type is Errors.GroupAuthorizationFailedError:
            error = error_type(self.group_id)
            log.error("Group Coordinator Request failed: %s", error)
            future.failure(error)
        else:
            error = error_type()
            log.error("Unrecognized failure in Group Coordinator Request: %s",
                      error)
            future.failure(error)

    def coordinator_dead(self, error=None):
        """Mark the current coordinator as dead."""
        if self.coordinator_id is not None:
            log.info("Marking the coordinator dead (node %s): %s.",
                     self.coordinator_id, error)
            self.coordinator_id = None

    def close(self):
        """Close the coordinator, leave the current group
        and reset local generation/memberId."""
        try:
            self._client.unschedule(self.heartbeat_task)
        except KeyError:
            pass
        if not self.coordinator_unknown() and self.generation > 0:
            # this is a minimal effort attempt to leave the group. we do not
            # attempt any resending if the request fails or times out.
            request = LeaveGroupRequest(self.group_id, self.member_id)
            future = self._client.send(self.coordinator_id, request)
            future.add_callback(self._handle_leave_group_response)
            future.add_errback(log.error, "LeaveGroup request failed: %s")
            self._client.poll(future=future)

        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.rejoin_needed = True

    def _handle_leave_group_response(self, response):
        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            log.info("LeaveGroup request succeeded")
        else:
            log.error("LeaveGroup request failed: %s", error_type())

    def send_heartbeat_request(self):
        """Send a heartbeat request now (visible only for testing)."""
        request = HeartbeatRequest(self.group_id, self.generation, self.member_id)
        future = Future()
        _f = self._client.send(self.coordinator_id, request)
        _f.add_callback(self._handle_heartbeat_response, future)
        _f.add_errback(self._failed_request, future)
        return future

    def _handle_heartbeat_response(self, future, response):
        #self.sensors.heartbeat_latency.record(response.requestLatencyMs())
        error_type = Errors.for_code(response.error_code)
        if error_type is Errors.NoError:
            log.debug("Received successful heartbeat response.")
            future.success(None)
        elif error_type in (Errors.GroupCoordinatorNotAvailableError,
                            Errors.NotCoordinatorForGroupError):
            log.info("Attempt to heart beat failed since coordinator is either"
                     " not started or not valid; marking it as dead.")
            self.coordinator_dead()
            future.failure(error_type())
        elif error_type is Errors.RebalanceInProgressError:
            log.info("Attempt to heart beat failed since the group is"
                     " rebalancing; try to re-join group.")
            self.rejoin_needed = True
            future.failure(error_type())
        elif error_type is Errors.IllegalGenerationError:
            log.info("Attempt to heart beat failed since generation id"
                     " is not legal; try to re-join group.")
            self.rejoin_needed = True
            future.failure(error_type())
        elif error_type is Errors.UnknownMemberIdError:
            log.info("Attempt to heart beat failed since member id"
                     " is not valid; reset it and try to re-join group.")
            self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
            self.rejoin_needed = True
            future.failure(error_type)
        elif error_type is Errors.GroupAuthorizationFailedError:
            error = error_type(self.group_id)
            log.error("Attempt to heart beat failed authorization: %s", error)
            future.failure(error)
        else:
            error = error_type()
            log.error("Unknown error in heart beat response: %s", error)
            future.failure(error)


class HeartbeatTask(object):
    def __init__(self, coordinator):
        self._coordinator = coordinator
        self._heartbeat = coordinator.heartbeat
        self._client = coordinator._client
        self._request_in_flight = False

    def reset(self):
        # start or restart the heartbeat task to be executed at the next chance
        self._heartbeat.reset_session_timeout()
        try:
            self._client.unschedule(self)
        except KeyError:
            pass
        if not self._request_in_flight:
            self._client.schedule(self, time.time())

    def __call__(self):
        log.debug("Running Heartbeat task")
        if (self._coordinator.generation < 0 or
            self._coordinator.need_rejoin() or
            self._coordinator.coordinator_unknown()):
            # no need to send the heartbeat we're not using auto-assignment
            # or if we are awaiting a rebalance
            log.debug("Skipping heartbeat: no auto-assignment"
                      " or waiting on rebalance")
            return

        if self._heartbeat.session_expired():
            # we haven't received a successful heartbeat in one session interval
            # so mark the coordinator dead
            log.error("Heartbeat session expired")
            self._coordinator.coordinator_dead()
            return

        if not self._heartbeat.should_heartbeat():
            # we don't need to heartbeat now, so reschedule for when we do
            ttl = self._heartbeat.ttl()
            log.debug("Heartbeat unneeded now, retrying in %s", ttl)
            self._client.schedule(self, time.time() + ttl)
        else:
            log.debug("Sending HeartbeatRequest")
            self._heartbeat.sent_heartbeat()
            self._request_in_flight = True
            future = self._coordinator.send_heartbeat_request()
            future.add_callback(self._handle_heartbeat_success)
            future.add_errback(self._handle_heartbeat_failure)

    def _handle_heartbeat_success(self, v):
        log.debug("Received successful heartbeat")
        self._request_in_flight = False
        self._heartbeat.received_heartbeat()
        ttl = self._heartbeat.ttl()
        self._client.schedule(self, time.time() + ttl)

    def _handle_heartbeat_failure(self, e):
        log.debug("Heartbeat failed; retrying")
        self._request_in_flight = False
        etd = time.time() + self._coordinator._retry_backoff_ms / 1000.0
        self._client.schedule(self, etd)


class GroupCoordinatorMetrics(object):
    def __init__(self, metrics, prefix, tags=None):
        self.metrics = metrics
        self.group_name = prefix + "-coordinator-metrics"

        self.heartbeat_latency = metrics.sensor("heartbeat-latency")
        self.heartbeat_latency.add(metrics.metricName(
            "heartbeat-response-time-max", self.group_name,
            "The max time taken to receive a response to a heartbeat request",
            tags), metrics.Max())
        self.heartbeat_latency.add(metrics.metricName(
            "heartbeat-rate", self.group_name,
            "The average number of heartbeats per second",
            tags), metrics.Rate(metrics.Count()))

        self.join_latency = metrics.sensor("join-latency")
        self.join_latency.add(metrics.metricName(
            "join-time-avg", self.group_name,
            "The average time taken for a group rejoin",
            tags), metrics.Avg())
        self.join_latency.add(metrics.metricName(
            "join-time-max", self.group_name,
            "The max time taken for a group rejoin",
            tags), metrics.Avg())
        self.join_latency.add(metrics.metricName(
            "join-rate", self.group_name,
            "The number of group joins per second",
            tags), metrics.Rate(metrics.Count()))

        self.sync_latency = metrics.sensor("sync-latency")
        self.sync_latency.add(metrics.metricName(
            "sync-time-avg", self.group_name,
            "The average time taken for a group sync",
            tags), metrics.Avg())
        self.sync_latency.add(metrics.MetricName(
            "sync-time-max", self.group_name,
            "The max time taken for a group sync",
            tags), metrics.Avg())
        self.sync_latency.add(metrics.metricName(
            "sync-rate", self.group_name,
            "The number of group syncs per second",
            tags), metrics.Rate(metrics.Count()))

        """
        lastHeartbeat = Measurable(
            measure=lambda _, value: value - heartbeat.last_heartbeat_send()
        )
        metrics.addMetric(metrics.metricName(
            "last-heartbeat-seconds-ago", self.group_name,
            "The number of seconds since the last controller heartbeat",
            tags), lastHeartbeat)
        """


