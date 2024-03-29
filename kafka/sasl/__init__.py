import logging

from kafka.sasl import gssapi, oauthbearer, plain, scram

MECHANISMS = {
    'GSSAPI': gssapi,
    'OAUTHBEARER': oauthbearer,
    'PLAIN': plain,
    'SCRAM-SHA-256': scram,
    'SCRAM-SHA-512': scram,
}

try:
    from kafka.sasl import msk
    MECHANISMS['AWS_MSK_IAM'] = msk
except ImportError:
    pass

log = logging.getLogger(__name__)


def register_mechanism(key, module):
    """
    Registers a custom SASL mechanism that can be used via sasl_mechanism={key}.

    Example:
        import kakfa.sasl
        from kafka import KafkaProducer
        from mymodule import custom_sasl
        kafka.sasl.register_mechanism('CUSTOM_SASL', custom_sasl)

        producer = KafkaProducer(sasl_mechanism='CUSTOM_SASL')

    Arguments:
        key (str): The name of the mechanism returned by the broker and used
            in the sasl_mechanism config value.
        module (module): A module that implements the following methods...

            def validate_config(conn: BrokerConnection): -> None:
                # Raises an AssertionError for missing or invalid conifg values.

            def try_authenticate(conn: BrokerConncetion, future: -> Future):
                # Executes authentication routine and returns a resolved Future.

    Raises:
        AssertionError: The registered module does not define a required method.
    """
    assert callable(getattr(module, 'validate_config', None)), (
        'Custom SASL mechanism {} must implement method #validate_config()'
        .format(key)
    )
    assert callable(getattr(module, 'try_authenticate', None)), (
        'Custom SASL mechanism {} must implement method #try_authenticate()'
        .format(key)
    )
    if key in MECHANISMS:
        log.warning(f'Overriding existing SASL mechanism {key}')

    MECHANISMS[key] = module
