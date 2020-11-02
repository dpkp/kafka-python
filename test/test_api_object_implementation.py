import abc
import pytest

from kafka.protocol.api import Request
from kafka.protocol.api import Response


attr_names = [n for n in dir(Request) if isinstance(getattr(Request, n), abc.abstractproperty)]
@pytest.mark.parametrize('klass', Request.__subclasses__())
@pytest.mark.parametrize('attr_name', attr_names)
def test_request_type_conformance(klass, attr_name):
    assert hasattr(klass, attr_name)

attr_names = [n for n in dir(Response) if isinstance(getattr(Response, n), abc.abstractproperty)]
@pytest.mark.parametrize('klass', Response.__subclasses__())
@pytest.mark.parametrize('attr_name', attr_names)
def test_response_type_conformance(klass, attr_name):
    assert hasattr(klass, attr_name)
