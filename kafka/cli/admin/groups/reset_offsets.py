import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from kafka.admin import OffsetSpec, OffsetTimestamp
from kafka.structs import TopicPartition


class ResetGroupOffsets:
    COMMAND = 'reset-offsets'
    HELP = 'Reset committed offsets for a consumer group'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.add_argument(
            '-p', '--partition', type=str, action='append',
            dest='partitions', default=[],
            help='TOPIC:PARTITION pair (repeatable). Scopes the reset to these '
                 'partitions. If omitted, the reset applies to every partition '
                 'currently committed by the group.')
        mode = parser.add_mutually_exclusive_group(required=True)
        mode.add_argument(
            '-s', '--spec', type=str,
            help='Spec may be one of earliest, latest, max-timestamp, '
                 'earliest-local, latest-tiered, or a millisecond timestamp.')
        mode.add_argument(
            '--to-offset', type=int, dest='to_offset',
            help='Reset all in-scope partitions to this explicit offset '
                 '(clamped to [earliest, latest]).')
        mode.add_argument(
            '--shift-by', type=int, dest='shift_by',
            help='Shift each in-scope committed offset by N positions (may be '
                 'negative); clamped to [earliest, latest]. Requires a current '
                 'commit for each in-scope partition.')
        mode.add_argument(
            '--by-duration', type=str, dest='by_duration',
            help='Reset to the offset at (now - DURATION). DURATION is ISO-8601, '
                 'e.g. P7D, PT1H, PT30M.')
        mode.add_argument(
            '--to-datetime', type=str, dest='to_datetime',
            help='Reset to the offset at the given ISO-8601 datetime (UTC assumed '
                 'if no tz offset is provided).')
        mode.add_argument(
            '--to-current', action='store_true', dest='to_current',
            help='Re-commit the current committed offsets, clamped to '
                 '[earliest, latest]. Useful to heal out-of-range offsets. '
                 'Requires a current commit for each in-scope partition.')

    @classmethod
    def command(cls, client, args):
        group = client.describe_groups([args.group_id])
        state = group[args.group_id]['group_state']
        if state not in ('Empty', 'Dead'):
            raise RuntimeError(f'Group {args.group_id} is {state}, expecting Empty or Dead!')

        targets = cls._build_targets(client, args)
        result = client.reset_group_offsets(args.group_id, targets)
        output = defaultdict(dict)
        for tp, res in result.items():
            res['error'] = res['error'].__name__
            output[tp.topic][tp.partition] = res
        return dict(output)

    @classmethod
    def _build_targets(cls, client, args):
        explicit_scope = [cls._parse_tp(p) for p in args.partitions] if args.partitions else None

        if args.spec is not None:
            scope = explicit_scope if explicit_scope is not None else list(client.list_group_offsets(args.group_id))
            spec = cls._parse_spec(args.spec)
            return {tp: spec for tp in scope}

        if args.to_offset is not None:
            scope = explicit_scope if explicit_scope is not None else list(client.list_group_offsets(args.group_id))
            return {tp: args.to_offset for tp in scope}

        if args.by_duration:
            delta = cls._parse_duration(args.by_duration)
            ts = OffsetTimestamp(int((datetime.now(timezone.utc) - delta).timestamp() * 1000))
            scope = explicit_scope if explicit_scope is not None else list(client.list_group_offsets(args.group_id))
            return {tp: ts for tp in scope}

        if args.to_datetime:
            dt = cls._parse_datetime(args.to_datetime)
            ts = OffsetTimestamp(int(dt.timestamp() * 1000))
            scope = explicit_scope if explicit_scope is not None else list(client.list_group_offsets(args.group_id))
            return {tp: ts for tp in scope}

        current = client.list_group_offsets(args.group_id)
        scope = explicit_scope if explicit_scope is not None else list(current)
        missing = [tp for tp in scope if tp not in current]
        if missing:
            raise ValueError(
                f'No committed offset for {missing}; --shift-by / --to-current '
                'require a current commit per in-scope partition')

        if args.shift_by is not None:
            return {tp: current[tp].offset + args.shift_by for tp in scope}
        if args.to_current:
            return {tp: current[tp].offset for tp in scope}
        raise ValueError('No reset mode selected')

    @classmethod
    def _parse_tp(cls, entry):
        topic, partition = entry.rsplit(':', 1)
        return TopicPartition(topic, int(partition))

    @classmethod
    def _parse_spec(cls, spec_str):
        try:
            return OffsetSpec.build_from(spec_str)
        except ValueError:
            return OffsetTimestamp(int(spec_str))

    @classmethod
    def _parse_duration(cls, duration):
        m = re.match(
            r'^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$',
            duration)
        if not m or not any(m.groups()):
            raise ValueError(f'Invalid ISO-8601 duration: {duration}')
        days, hours, mins, secs = m.groups()
        return timedelta(
            days=int(days) if days else 0,
            hours=int(hours) if hours else 0,
            minutes=int(mins) if mins else 0,
            seconds=float(secs) if secs else 0,
        )

    @classmethod
    def _parse_datetime(cls, dt_str):
        try:
            dt = datetime.fromisoformat(dt_str)
        except ValueError:
            raise ValueError(f'Invalid ISO-8601 datetime: {dt_str}')
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
