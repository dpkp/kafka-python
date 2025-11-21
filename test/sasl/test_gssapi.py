from __future__ import absolute_import

from unittest import mock

from kafka.sasl import get_sasl_mechanism
import kafka.sasl.gssapi


def test_gssapi():
    config = {
        'sasl_kerberos_domain_name': 'foo',
        'sasl_kerberos_service_name': 'bar',
    }
    client_ctx = mock.Mock()
    client_ctx.step.side_effect = [b'init', b'exchange', b'complete', b'xxxx']
    client_ctx.complete = False
    def mocked_message_wrapper(msg, *args):
        wrapped = mock.Mock()
        type(wrapped).message = mock.PropertyMock(return_value=msg)
        return wrapped
    client_ctx.unwrap.side_effect = mocked_message_wrapper
    client_ctx.wrap.side_effect = mocked_message_wrapper
    kafka.sasl.gssapi.gssapi = mock.Mock()
    kafka.sasl.gssapi.gssapi.SecurityContext.return_value = client_ctx
    gssapi = get_sasl_mechanism('GSSAPI')(**config)
    assert isinstance(gssapi, kafka.sasl.gssapi.SaslMechanismGSSAPI)
    client_ctx.step.assert_called_with(None)

    while not gssapi.is_done():
        send_token = gssapi.auth_bytes()
        receive_token = send_token # not realistic, but enough for testing
        if send_token == b'\x01ompletebar@foo': # final wrapped message
            receive_token = b'' # final message gets an empty response
        gssapi.receive(receive_token)
        if client_ctx.step.call_count == 3:
            client_ctx.complete = True

    assert gssapi.is_done()
    assert gssapi.is_authenticated()
