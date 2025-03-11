from __future__ import absolute_import

import platform

from kafka.sasl.gssapi import SaslMechanismGSSAPI
from kafka.sasl.msk import SaslMechanismAwsMskIam
from kafka.sasl.oauth import SaslMechanismOAuth
from kafka.sasl.plain import SaslMechanismPlain
from kafka.sasl.scram import SaslMechanismScram
from kafka.sasl.sspi import SaslMechanismSSPI


SASL_MECHANISMS = {}


def register_sasl_mechanism(name, klass, overwrite=False):
    if not overwrite and name in SASL_MECHANISMS:
        raise ValueError('Sasl mechanism %s already defined!' % name)
    SASL_MECHANISMS[name] = klass


def get_sasl_mechanism(name):
    return SASL_MECHANISMS[name]


register_sasl_mechanism('AWS_MSK_IAM', SaslMechanismAwsMskIam)
if platform.system() == 'Windows':
    register_sasl_mechanism('GSSAPI', SaslMechanismSSPI)
else:
    register_sasl_mechanism('GSSAPI', SaslMechanismGSSAPI)
register_sasl_mechanism('OAUTHBEARER', SaslMechanismOAuth)
register_sasl_mechanism('PLAIN', SaslMechanismPlain)
register_sasl_mechanism('SCRAM-SHA-256', SaslMechanismScram)
register_sasl_mechanism('SCRAM-SHA-512', SaslMechanismScram)
