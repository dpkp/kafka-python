import sys

USAGE = """\
usage: kafka-python {admin,consumer,producer} [options]

Top-level entry point for the kafka-python command-line tools.

  kafka-python admin     -- administrative client (topics, configs, groups, ...)
  kafka-python consumer  -- console consumer
  kafka-python producer  -- console producer

Run `kafka-python <subcommand> --help` for subcommand-specific options.
"""


def run_cli(args=None):
    if args is None:
        args = sys.argv[1:]

    if not args or args[0] in ('-h', '--help'):
        sys.stdout.write(USAGE)
        return 0 if args else 1

    sub, rest = args[0], args[1:]
    if sub == 'admin':
        from kafka.cli.admin import run_cli as sub_run
    elif sub == 'consumer':
        from kafka.cli.consumer import run_cli as sub_run
    elif sub == 'producer':
        from kafka.cli.producer import run_cli as sub_run
    else:
        sys.stderr.write('kafka-python: unknown subcommand: %r\n' % sub)
        sys.stderr.write(USAGE)
        return 2

    return sub_run(rest, prog='kafka-python %s' % sub)
