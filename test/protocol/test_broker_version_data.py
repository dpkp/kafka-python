"""Tests for BrokerVersionData — version resolution, construction, and edge cases."""

import pytest

from kafka.errors import IncompatibleBrokerVersion, KafkaConfigurationError, UnrecognizedBrokerVersion
from kafka.protocol.broker_version_data import BrokerVersionData, BROKER_API_VERSIONS
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.admin import CreateTopicsRequest, CreatePartitionsRequest
from kafka.protocol.consumer import FetchRequest
from kafka.protocol.producer import ProduceRequest


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestBrokerVersionDataConstruction:
    def test_from_broker_version_tuple(self):
        bvd = BrokerVersionData((2, 8))
        assert bvd.broker_version == (2, 8)
        assert bvd.api_versions is not None
        assert isinstance(bvd.api_versions, dict)

    def test_from_api_versions_dict(self):
        api_versions = {0: (0, 9), 1: (0, 11), 3: (0, 9)}
        bvd = BrokerVersionData(api_versions=api_versions)
        assert bvd.api_versions == api_versions
        assert bvd.broker_version is not None  # inferred

    def test_no_args_raises(self):
        with pytest.raises(ValueError):
            BrokerVersionData()

    def test_invalid_string_raises(self):
        with pytest.raises((ValueError, KafkaConfigurationError)):
            BrokerVersionData(broker_version="not-a-tuple")

    def test_invalid_type_raises(self):
        with pytest.raises(KafkaConfigurationError):
            BrokerVersionData(broker_version=[2, 8])

    def test_unknown_version_falls_back(self):
        # A version that doesn't exist exactly should fall back to the
        # nearest known version that is <=.
        bvd = BrokerVersionData((4, 99))
        assert bvd.broker_version <= (4, 99)
        assert bvd.api_versions is not None

    def test_unrecognized_ancient_version_raises(self):
        with pytest.raises(UnrecognizedBrokerVersion):
            BrokerVersionData((0, 1))

    def test_string_version_accepted_with_warning(self):
        # Legacy string format still works
        bvd = BrokerVersionData('2.8.2')
        assert bvd.broker_version == (2, 8)

    def test_patched_version_normalized(self):
        # (0, 10) is ambiguous; should normalize to (0, 10, 0)
        bvd = BrokerVersionData((0, 10))
        assert bvd.broker_version == (0, 10, 0)

    def test_all_known_versions_construct(self):
        for version in BROKER_API_VERSIONS:
            bvd = BrokerVersionData(version)
            assert bvd.broker_version == version


# ---------------------------------------------------------------------------
# api_version resolution
# ---------------------------------------------------------------------------


class TestApiVersionResolution:
    @pytest.fixture
    def bvd(self):
        return BrokerVersionData((4, 2))

    def test_basic_resolution(self, bvd):
        version = bvd.api_version(MetadataRequest)
        assert isinstance(version, int)
        assert MetadataRequest.min_version <= version <= MetadataRequest.max_version

    def test_max_version_cap(self, bvd):
        # Caller-provided cap
        version = bvd.api_version(MetadataRequest, max_version=3)
        assert version <= 3

    def test_min_version_floor(self, bvd):
        version = bvd.api_version(MetadataRequest, min_version=5)
        assert version >= 5

    def test_min_exceeds_broker_max_raises(self):
        # Use an old broker that only supports low versions
        bvd = BrokerVersionData((0, 9, 0, 1))
        # FetchRequest on 0.9 supports some versions, but min_version=999 can't be satisfied
        with pytest.raises(IncompatibleBrokerVersion):
            bvd.api_version(FetchRequest, min_version=999)

    def test_max_below_broker_min_raises(self, bvd):
        # ProduceRequest on 4.2 has some min version; cap below it
        broker_min = bvd.api_versions[ProduceRequest.API_KEY][0]
        if broker_min > 0:
            with pytest.raises(IncompatibleBrokerVersion):
                bvd.api_version(ProduceRequest, max_version=broker_min - 1)

    def test_unsupported_api_raises(self):
        # Construct a BrokerVersionData with a minimal api_versions dict
        # that doesn't include CreateTopicsRequest
        bvd = BrokerVersionData(api_versions={0: (0, 9)})  # only ProduceRequest
        with pytest.raises(IncompatibleBrokerVersion, match="does not support"):
            bvd.api_version(CreateTopicsRequest)

    def test_min_max_inversion_asserts(self, bvd):
        with pytest.raises(AssertionError):
            bvd.api_version(MetadataRequest, min_version=10, max_version=3)

    def test_request_instance_max_version(self, bvd):
        """Per-instance _max_version on a request caps the resolved version."""
        # MetadataRequest on (4,2) broker supports v0-v13; cap to v3
        request = MetadataRequest(topics=None, max_version=3)
        version = bvd.api_version(request)
        assert version <= 3

    def test_request_instance_min_version(self, bvd):
        """Per-instance _min_version on a request sets a floor."""
        request = MetadataRequest(topics=None, min_version=5)
        version = bvd.api_version(request)
        assert version >= 5

    def test_request_instance_min_max_version(self, bvd):
        """Per-instance min and max narrow the range."""
        request = MetadataRequest(topics=None, min_version=3, max_version=5)
        version = bvd.api_version(request)
        assert 3 <= version <= 5

    def test_request_instance_version_no_override(self, bvd):
        """Request without instance min/max uses full range."""
        request = MetadataRequest(topics=None)
        version = bvd.api_version(request)
        assert version == min(MetadataRequest.max_version,
                              bvd.api_versions[MetadataRequest.API_KEY][1])

    def test_caller_max_and_instance_max_both_applied(self, bvd):
        """The effective max is min(caller_max, instance_max, class_max, broker_max)."""
        # MetadataRequest on (4,2) supports v0-v13
        request = MetadataRequest(topics=None, max_version=8)
        # Caller provides a tighter cap than the instance
        version = bvd.api_version(request, max_version=5)
        assert version <= 5

        # Instance provides a tighter cap than the caller
        request2 = MetadataRequest(topics=None, max_version=3)
        version2 = bvd.api_version(request2, max_version=8)
        assert version2 <= 3

    def test_class_vs_instance(self, bvd):
        """Passing the class (not an instance) should work without instance min/max."""
        version_from_class = bvd.api_version(MetadataRequest)
        version_from_instance = bvd.api_version(MetadataRequest(topics=None))
        assert version_from_class == version_from_instance


# ---------------------------------------------------------------------------
# Comparison and string representation
# ---------------------------------------------------------------------------


class TestBrokerVersionDataComparison:
    def test_equality(self):
        a = BrokerVersionData((2, 8))
        b = BrokerVersionData((2, 8))
        assert a == b

    def test_inequality(self):
        a = BrokerVersionData((2, 8))
        b = BrokerVersionData((3, 0))
        assert a != b

    def test_ordering(self):
        a = BrokerVersionData((2, 8))
        b = BrokerVersionData((3, 0))
        assert a < b
        assert b > a

    def test_str(self):
        bvd = BrokerVersionData((2, 8))
        assert '2.8' in str(bvd)
