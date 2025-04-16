import datetime
import json
import sys

from kafka.sasl.msk import AwsMskIamClient

try:
    from unittest import mock
except ImportError:
    import mock


def client_factory(token=None):
    if sys.version_info >= (3, 3):
        now = datetime.datetime.fromtimestamp(1629321911, datetime.timezone.utc)
    else:
        now = datetime.datetime.utcfromtimestamp(1629321911)
    with mock.patch('kafka.sasl.msk.datetime') as mock_dt:
        mock_dt.datetime.utcnow = mock.Mock(return_value=now)
        return AwsMskIamClient(
            host='localhost',
            access_key='XXXXXXXXXXXXXXXXXXXX',
            secret_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
            region='us-east-1',
            token=token,
        )


def test_aws_msk_iam_client_permanent_credentials():
    client = client_factory(token=None)
    msg = client.first_message()
    assert msg
    assert isinstance(msg, bytes)
    actual = json.loads(msg)

    expected = {
        'version': '2020_10_22',
        'host': 'localhost',
        'user-agent': 'kafka-python',
        'action': 'kafka-cluster:Connect',
        'x-amz-algorithm': 'AWS4-HMAC-SHA256',
        'x-amz-credential': 'XXXXXXXXXXXXXXXXXXXX/20210818/us-east-1/kafka-cluster/aws4_request',
        'x-amz-date': '20210818T212511Z',
        'x-amz-signedheaders': 'host',
        'x-amz-expires': '900',
        'x-amz-signature': '0fa42ae3d5693777942a7a4028b564f0b372bafa2f71c1a19ad60680e6cb994b',
    }
    assert actual == expected


def test_aws_msk_iam_client_temporary_credentials():
    client = client_factory(token='XXXXX')
    msg = client.first_message()
    assert msg
    assert isinstance(msg, bytes)
    actual = json.loads(msg)

    expected = {
        'version': '2020_10_22',
        'host': 'localhost',
        'user-agent': 'kafka-python',
        'action': 'kafka-cluster:Connect',
        'x-amz-algorithm': 'AWS4-HMAC-SHA256',
        'x-amz-credential': 'XXXXXXXXXXXXXXXXXXXX/20210818/us-east-1/kafka-cluster/aws4_request',
        'x-amz-date': '20210818T212511Z',
        'x-amz-signedheaders': 'host',
        'x-amz-expires': '900',
        'x-amz-signature': 'b0619c50b7ecb4a7f6f92bd5f733770df5710e97b25146f97015c0b1db783b05',
        'x-amz-security-token': 'XXXXX',
    }
    assert actual == expected
