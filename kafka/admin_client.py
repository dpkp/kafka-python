import time
from .errors import NodeNotReadyError
from .protocol.admin import CreateTopicsRequest, DeleteTopicsRequest
from .protocol.metadata import MetadataRequest

def convert_new_topic_request_format(new_topic):
    return (
        new_topic.name,
        new_topic.num_partitions,
        new_topic.replication_factor,
        [
            (partition_id,replicas)
            for partition_id, replicas in new_topic.replica_assignments.items()
        ],
        [
            (config_key, config_value)
            for config_key, config_value in new_topic.configs.items()
        ],
    )

class NewTopic(object):
    """ A class for new topic creation

    Arguments:
        name (string): name of the topic
        num_partitions (int): number of partitions
            or -1 if replica_assignment has been specified
        replication_factor (int): replication factor or -1 if
            replica assignment is specified
        replica_assignment (dict of int: [int]): A mapping containing
            partition id and replicas to assign to it.
        topic_configs (dict of str: str): A mapping of config key
            and value for the topic.
    """

    def __init__(
        self,
        name,
        num_partitions,
        replication_factor,
        replica_assignments=None,
        configs=None,
    ):
        self.name = name
        self.configs = configs or {}
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignments = replica_assignments or {}

    def __str__(self):
        return "<name>:{}, <num_partitions>:{}, <replication_factor>:{}" \
            "<replica_assignments>:{}, <configs>:{}".format(
            self.name,
            self.num_partitions,
            self.replication_factor,
            self.replica_assignments,
            self.configs,
        )

class AdminClient(object):
    """
    An api to send CreateTopic requests
    
    """    
    def __init__(self, client):
        self.client = client
        self.metadata_request = MetadataRequest[1]([])
        self.topic_request = CreateTopicsRequest[0]
        self.delete_topics_request = DeleteTopicsRequest[0]

    def _send_controller_request(self):
        response = self._send(
            self.client.least_loaded_node(),
            self.metadata_request,
        )
        return response[0].controller_id
        
    def _send(self, node, request):
        future = self.client.send(node, request)
        return self.client.poll(future=future)

    def _send_request(self, request):
        controller_id = self._send_controller_request()
        if not self.client.ready(controller_id):
            raise NodeNotReadyError(controller_id)
        else:
            return self._send(controller_id, request)
        
     
    def create_topics(
        self,
        topics, 
        timeout, 
    ):
        """ Create topics on the cluster 

        Arguments:
            new_topics (list of NewTopic): A list containing new 
                topics to be created
            validate_only (bool): True if we just want to validate the request
            timeout (int): timeout in seconds 
            max_retry (int): num of times we want to retry to send a create
                topic request when the controller in not available

        Returns:
            CreateTopicResponse: response from the broker
        
        Raises: 
            NodeNotReadyError: if controller is not ready 
        """
        request = self.topic_request(
            create_topic_requests=[
                convert_new_topic_request_format(topic)
                for topic in topics
            ],
            timeout=timeout,
        ) 
        return self._send_request(request) 

    def delete_topics(self, topics, timeout):
        """ Deletes topics on the cluster
        
        Arguments:
            topics (list of topic names): Topics to delete
            timeout (int): The requested timeout for this operation
        Raises:
            NodeNotReadyError: if retry exceeds max_retry
        """

        request = self.delete_topics_request(
            topics=topics,
            timeout=timeout,
        )
        return self._send_request(request)
         
    
