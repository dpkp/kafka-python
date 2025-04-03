from __future__ import absolute_import

import pytest

from kafka import TopicPartition
from kafka.consumer.subscription_state import SubscriptionState, TopicPartitionState
from kafka.vendor import six


def test_type_error():
    s = SubscriptionState()
    with pytest.raises(TypeError):
        s.subscribe(topics='foo')

    s.subscribe(topics=['foo'])


def test_change_subscription():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    assert s.subscription == set(['foo'])
    s.change_subscription(['bar'])
    assert s.subscription == set(['bar'])


def test_group_subscribe():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    assert s.subscription == set(['foo'])
    s.group_subscribe(['bar'])
    assert s.subscription == set(['foo'])
    assert s._group_subscription == set(['foo', 'bar'])

    s.reset_group_subscription()
    assert s.subscription == set(['foo'])
    assert s._group_subscription == set(['foo'])


def test_assign_from_subscribed():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    with pytest.raises(ValueError):
        s.assign_from_subscribed([TopicPartition('bar', 0)])

    s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
    assert set(s.assignment.keys()) == set([TopicPartition('foo', 0), TopicPartition('foo', 1)])
    assert all([isinstance(tps, TopicPartitionState) for tps in six.itervalues(s.assignment)])
    assert all([not tps.has_valid_position for tps in six.itervalues(s.assignment)])


def test_change_subscription_after_assignment():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
    # Changing subscription retains existing assignment until next rebalance
    s.change_subscription(['bar'])
    assert set(s.assignment.keys()) == set([TopicPartition('foo', 0), TopicPartition('foo', 1)])
