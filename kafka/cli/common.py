def add_common_cli_args(parser):
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
