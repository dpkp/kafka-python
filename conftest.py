import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--metadata-timeout-ms",
        action="store",
        default=60000,
        type=int,
        help="Timeout in milliseconds for metadata requests during tests"
    )
    parser.addoption(
        "--acks-test-timeout",
        action="store",
        default=60000,
        type=int,
        help="Timeout in milliseconds for producer acks tests"
    )

@pytest.fixture
def metadata_timeout_ms(request):
    return request.config.getoption("--metadata-timeout-ms")

@pytest.fixture
def acks_test_timeout(request):
    return request.config.getoption("--acks-test-timeout")
