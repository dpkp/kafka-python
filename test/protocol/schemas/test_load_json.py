from kafka.protocol.schemas import load_json


def test_load_json_common_structs():
    # AddPartitionsToTxnRequest uses commonStructs for 'AddPartitionsToTxnTopic'
    data = load_json('AddPartitionsToTxnRequest')

    assert 'commonStructs' in data
    common_names = [s['name'] for s in data['commonStructs']]
    assert 'AddPartitionsToTxnTopic' in common_names

    # Find V3AndBelowTopics field - it should have fields injected
    v3_topics = next(f for f in data['fields'] if f['name'] == 'V3AndBelowTopics')
    assert 'fields' in v3_topics
    assert any(f['name'] == 'Partitions' for f in v3_topics['fields'])

    # Find Transactions field (v4+)
    transactions = next(f for f in data['fields'] if f['name'] == 'Transactions')
    # It has a nested 'Topics' field which should also have fields injected
    nested_topics = next(f for f in transactions['fields'] if f['name'] == 'Topics')
    assert 'fields' in nested_topics
    assert any(f['name'] == 'Partitions' for f in nested_topics['fields'])


def test_load_json_basic():
    # Simple check for a message without commonStructs
    data = load_json('ApiVersionsRequest')
    assert 'fields' in data
    assert 'commonStructs' not in data or len(data['commonStructs']) == 0
