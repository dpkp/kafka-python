import logging


def add_connect_cli_args(parser):
    connect_group = parser.add_argument_group('connection')
    connect_group.add_argument(
        '-b', '--bootstrap-servers', type=str, action='append', required=True,
        help='host:port for cluster bootstrap server. Can be provided multiple times.')
    connect_group.add_argument(
        '-s', '--security-protocol', type=str, default='PLAINTEXT', help='PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL')
    connect_group.add_argument(
        '-m', '--sasl-mechanism', type=str, default='PLAIN', help='PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512')
    connect_group.add_argument(
        '-u', '--sasl-user', type=str)
    connect_group.add_argument(
        '-p', '--sasl-password', type=str)


def build_kwargs(props):
    kwargs = {}
    for prop in props or []:
        k, v = prop.split('=')
        try:
            v = int(v)
        except ValueError:
            pass
        if v == 'None':
            v = None
        elif v == 'False':
            v = False
        elif v == 'True':
            v = True
        kwargs[k] = v
    return kwargs


def build_connect_kwargs(config):
    kwargs = build_kwargs(config.extra_config)
    kwargs.update({
        'bootstrap_servers': config.bootstrap_servers,
        'security_protocol': config.security_protocol,
        'sasl_mechanism': config.sasl_mechanism,
        'sasl_plain_username': config.sasl_user,
        'sasl_plain_password': config.sasl_password,
    })
    return kwargs


def add_logging_cli_args(parser):
    logging_group = parser.add_argument_group('logging')
    logging_group.add_argument(
        '-l', '--log-level', type=str, default='CRITICAL',
        help='logging level, passed to logging.basicConfig')
    logging_group.add_argument(
        '-L', '--enable-logger', type=str, action='append',
        help='enable a specific logger. Can be provided multiple times. If not provided, all loggers are enabled')
    logging_group.add_argument(
        '-DL', '--disable-logger', type=str, action='append',
        help='disable a specific logger. Can be provided multiple times.')
    extended_group = parser.add_argument_group('extended')
    extended_group.add_argument(
        '-c', '--extra-config', type=str, action='append',
        help='additional configuration properties for client in "key=val" format. Can be provided multiple times.')


def configure_logging(config):
    _LOGGING_LEVELS = {
        'NOTSET': 0,
        'DEBUG': 10,
        'INFO': 20,
        'WARNING': 30,
        'ERROR': 40,
        'CRITICAL': 50,
    }
    if config.enable_logger is not None:
        log_level = _LOGGING_LEVELS[config.log_level.upper()]
        handler = logging.StreamHandler()
        handler.setLevel(log_level)
        handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        for name in config.enable_logger:
            logger = logging.getLogger(name)
            logger.setLevel(log_level)
            logger.addHandler(handler)
    else:
        logging.basicConfig(level=_LOGGING_LEVELS[config.log_level.upper()])
    if config.disable_logger is not None:
        for name in config.disable_logger:
            logging.getLogger(name).setLevel(logging.CRITICAL + 1)


def add_common_cli_args(parser):
    add_connect_cli_args(parser)
    add_logging_cli_args(parser)
