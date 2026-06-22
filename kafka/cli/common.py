import logging
import logging.config


def add_connect_cli_args(parser, bootstrap_required=True):
    connect_group = parser.add_argument_group('connection')
    connect_group.add_argument(
        '-b', '--bootstrap-servers', type=str, action='append', required=bootstrap_required,
        help='host:port for cluster bootstrap server. Can be provided multiple times.')
    connect_group.add_argument(
        '-S', '--security-protocol', type=str, default='PLAINTEXT', help='PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL')
    connect_group.add_argument(
        '-M', '--sasl-mechanism', type=str, default='PLAIN', help='PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512')
    connect_group.add_argument(
        '-U', '--sasl-user', type=str)
    connect_group.add_argument(
        '-P', '--sasl-password', type=str)


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
    if not config.bootstrap_servers:
        raise ValueError('python -m kafka: error: the following arguments are required: -b/--bootstrap-servers')
    # Accept both repeated -b flags and comma-separated lists within a single flag
    bootstrap_servers = []
    for entry in config.bootstrap_servers:
        bootstrap_servers.extend(s.strip() for s in entry.split(',') if s.strip())
    kwargs = build_kwargs(config.extra_config)
    kwargs.update({
        'bootstrap_servers': bootstrap_servers,
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
        '-D', '--disable-logger', type=str, action='append',
        help='disable a specific logger. Can be provided multiple times.')
    logging_group.add_argument(
        '--log-format', type=str, default=None,
        help='log message format string, passed to logging.Formatter')
    logging_group.add_argument(
        '--log-date-format', type=str, default=None,
        help='log date format string, passed to logging.Formatter')
    logging_group.add_argument(
        '--log-file', type=str, default=None,
        help='write logs to this file instead of stderr')
    logging_group.add_argument(
        '--log-config', type=str, default=None,
        help='path to a logging configuration file for full control over handlers, '
             'formatters, etc. A .json (or .yaml/.yml, if PyYAML is installed) file is '
             'loaded as a logging.config.dictConfig; any other extension is loaded as a '
             'logging.config.fileConfig. The file owns handlers/formatters, so --log-format, '
             '--log-date-format and --log-file are ignored, but --enable-logger and '
             '--disable-logger still apply as logger level adjustments.')


def add_extended_cli_args(parser):
    extended_group = parser.add_argument_group('extended')
    extended_group.add_argument(
        '-C', '--extra-config', type=str, action='append',
        help='additional configuration properties for client in "key=val" format. Can be provided multiple times.')


def _load_log_config(path):
    """Configure logging from a dictConfig (.json/.yaml) or fileConfig (.ini) file."""
    if path.endswith(('.yaml', '.yml')):
        try:
            import yaml
        except ImportError:
            raise ValueError('PyYAML is required to load a YAML logging config: %s' % (path,))
        with open(path) as f:
            _dict_config(yaml.safe_load(f))
    elif path.endswith('.json'):
        import json
        with open(path) as f:
            _dict_config(json.load(f))
    else:
        # disable_existing_loggers defaults to True, which would silence loggers
        # configured before this call (e.g. the loggers we are about to enable);
        # keep them around so --enable-logger still works.
        logging.config.fileConfig(path, disable_existing_loggers=False)


def _dict_config(cfg):
    # Default disable_existing_loggers to False (dictConfig defaults to True), so a
    # config that does not mention an already-created logger does not silence it.
    # A config may still set it explicitly to opt into the stdlib default.
    cfg.setdefault('disable_existing_loggers', False)
    logging.config.dictConfig(cfg)


def configure_logging(config):
    _LOGGING_LEVELS = {
        'NOTSET': 0,
        'DEBUG': 10,
        'INFO': 20,
        'WARNING': 30,
        'ERROR': 40,
        'CRITICAL': 50,
    }
    log_level = _LOGGING_LEVELS[config.log_level.upper()]
    if getattr(config, 'log_config', None):
        _load_log_config(config.log_config)
        if config.enable_logger is not None:
            # Preserve the no-config behavior of --enable-logger: ONLY the named
            # loggers emit. The config file owns the handlers/formatters, so rather
            # than attach our own we reuse them: silence everything else by raising
            # the root level, then let each enabled logger opt back in and propagate
            # its records up to the config's handlers.
            logging.getLogger().setLevel(logging.CRITICAL + 1)
            for name in config.enable_logger:
                logger = logging.getLogger(name)
                logger.disabled = False
                logger.setLevel(log_level)
    else:
        log_format = config.log_format or logging.BASIC_FORMAT
        if config.enable_logger is not None:
            if config.log_file:
                handler = logging.FileHandler(config.log_file)
            else:
                handler = logging.StreamHandler()
            handler.setLevel(log_level)
            handler.setFormatter(logging.Formatter(log_format, datefmt=config.log_date_format))
            for name in config.enable_logger:
                logger = logging.getLogger(name)
                logger.setLevel(log_level)
                logger.addHandler(handler)
        else:
            logging.basicConfig(
                level=log_level, format=log_format,
                datefmt=config.log_date_format, filename=config.log_file)
    # --disable-logger silences a named logger in either mode by raising its level.
    for name in config.disable_logger or []:
        logging.getLogger(name).setLevel(logging.CRITICAL + 1)


def add_common_cli_args(parser, bootstrap_required=True):
    add_connect_cli_args(parser, bootstrap_required)
    add_logging_cli_args(parser)
    add_extended_cli_args(parser)
