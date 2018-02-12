# pylint: skip-file
from __future__ import absolute_import

import pytest

from kafka.consumer.subscription_state import SubscriptionState

@pytest.mark.parametrize(('topic_name', 'expectation'), [
    (0, pytest.raises(TypeError)),
    (None, pytest.raises(TypeError)),
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
    state = SubscriptionState()
    with expectation:
        state._ensure_valid_topic_name(topic_name)
