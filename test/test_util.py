# pylint: skip-file

import pytest

from kafka.util import ensure_valid_topic_name

@pytest.mark.parametrize(('topic_name', 'expectation'), [
    (0, pytest.raises(TypeError)),
    (None, pytest.raises(TypeError)),
    (b'bytes-topic', pytest.raises(TypeError)),
    ('', pytest.raises(ValueError)),
    ('.', pytest.raises(ValueError)),
    ('..', pytest.raises(ValueError)),
    ('a' * 250, pytest.raises(ValueError)),
    ('abc/123', pytest.raises(ValueError)),
    ('/abc/123', pytest.raises(ValueError)),
    ('/abc123', pytest.raises(ValueError)),
    ('name with space', pytest.raises(ValueError)),
    ('name*with*stars', pytest.raises(ValueError)),
    ('name+with+plus', pytest.raises(ValueError)),
])
def test_topic_name_validation(topic_name, expectation):
    with expectation:
        ensure_valid_topic_name(topic_name)


@pytest.mark.parametrize('topic_name', [
    'a',
    'topic',
    'my-topic',
    'my_topic',
    'my.topic',
    'topic123',
    'a' * 249,
])
def test_topic_name_validation_success(topic_name):
    assert ensure_valid_topic_name(topic_name) is None
