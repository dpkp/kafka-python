import mock
import pytest
from kafka.client_async import KafkaClient
from kafka.errors import BrokerNotAvailableError
from kafka.protocol.metadata import MetadataResponse
from kafka.protocol.admin import CreateTopicsResponse, DeleteTopicsResponse, CreatePartitionsResponse
from kafka.admin_client import AdminClient
from kafka.admin_client import NewTopic 
from kafka.admin_client import NewPartitionsInfo
from kafka.structs import BrokerMetadata
from kafka.future import Future

@pytest.fixture
def bootstrap_brokers():
    return 'fake-broker:9092'

@pytest.fixture
def controller_id():
    return 100

@pytest.fixture
def mock_least_loaded_node():
    return 2

@pytest.fixture
def metadata_response(controller_id):
    return [MetadataResponse[1](
        [(1,'host',80,'rack')], controller_id, 
        [(37,'topic',False,[(7,1,2,[1,2,3],[1,2,3])])]        
    )]

@pytest.fixture
def mock_new_topics():
    return [NewTopic('topic',1,1)] 

@pytest.fixture
def mock_topic_partitions():
    return [NewPartitionsInfo('topic', 5, 4*[[1,2,3]]) ]

@pytest.fixture
def topic_response():
    return CreateTopicsResponse[1]([(
        'topic',7,'timeout_exception'     
    )])

@pytest.fixture
def delete_response():
    return DeleteTopicsResponse[0]([(
        'topic',7
    )])

@pytest.fixture
def partition_response():
    return CreatePartitionsResponse[0](
        100,
        [('topic', 7, 'timeout_exception')]
    )

class TestTopicAdmin():

    def test_send_controller_request(
        self,
        mock_least_loaded_node, 
        controller_id, 
        bootstrap_brokers, 
        metadata_response
    ):  
        mock_kafka_client = mock.Mock() 
        mock_kafka_client.poll.return_value = metadata_response
        mock_kafka_client.least_loaded_node.return_value = \
                 mock_least_loaded_node
        mock_kafka_client.send.return_value = Future()
        mock_kafka_client.connected.return_value = True
        admin = AdminClient(mock_kafka_client)
        assert admin._send_controller_request() == controller_id

    def test_create_topics(
        self,
        mock_new_topics, 
        mock_least_loaded_node,
        bootstrap_brokers,
        topic_response,
        metadata_response, 
    ):
        mock_kafka_client = mock.Mock()
        mock_kafka_client.poll = \
            mock.Mock(side_effect=[metadata_response, topic_response])
        mock_kafka_client.ready.return_value = True
        mock_kafka_client.least_loaded_node.return_value = \
            mock_least_loaded_node
        mock_kafka_client.send.return_value = Future()
        admin = AdminClient(mock_kafka_client)
        response = admin.create_topics(mock_new_topics, 0)
        assert response == topic_response

    def test_delete_topics(
        self,
        mock_new_topics,
        mock_least_loaded_node,
        bootstrap_brokers,
        delete_response,
        metadata_response,
    ):
        mock_kafka_client = mock.Mock()
        mock_kafka_client.poll = \
            mock.Mock(side_effect=[metadata_response, delete_response])
        mock_kafka_client.ready.return_value = True
        mock_kafka_client.least_loaded_node.return_value = \
            mock_least_loaded_node
        mock_kafka_client.send.return_value = Future()
        admin = AdminClient(mock_kafka_client)
        response = admin.delete_topics(mock_new_topics, 0)
        assert response == delete_response

    def test_create_partitions(
        self,
        mock_topic_partitions,
        mock_least_loaded_node,
        partition_response,
        metadata_response,
    ):
        mock_kafka_client = mock.Mock()
        mock_kafka_client.poll = \
                mock.Mock(side_effect=[metadata_response, partition_response])
        mock_kafka_client.ready.return_value = True
        mock_kafka_client.least_loaded_node.return_value = \
                mock_least_loaded_node
        mock_kafka_client.send.return_value = Future()
        admin = AdminClient(mock_kafka_client)
        response = admin.create_partitions(mock_topic_partitions, 0, False)
        assert response == partition_response
