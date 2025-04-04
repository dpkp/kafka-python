#!/usr/bin/env python
from __future__ import print_function
import pyperf
from kafka.vendor import six


test_data = [
    (b"\x00", 0),
    (b"\x01", -1),
    (b"\x02", 1),
    (b"\x7E", 63),
    (b"\x7F", -64),
    (b"\x80\x01", 64),
    (b"\x81\x01", -65),
    (b"\xFE\x7F", 8191),
    (b"\xFF\x7F", -8192),
    (b"\x80\x80\x01", 8192),
    (b"\x81\x80\x01", -8193),
    (b"\xFE\xFF\x7F", 1048575),
    (b"\xFF\xFF\x7F", -1048576),
    (b"\x80\x80\x80\x01", 1048576),
    (b"\x81\x80\x80\x01", -1048577),
    (b"\xFE\xFF\xFF\x7F", 134217727),
    (b"\xFF\xFF\xFF\x7F", -134217728),
    (b"\x80\x80\x80\x80\x01", 134217728),
    (b"\x81\x80\x80\x80\x01", -134217729),
    (b"\xFE\xFF\xFF\xFF\x7F", 17179869183),
    (b"\xFF\xFF\xFF\xFF\x7F", -17179869184),
    (b"\x80\x80\x80\x80\x80\x01", 17179869184),
    (b"\x81\x80\x80\x80\x80\x01", -17179869185),
    (b"\xFE\xFF\xFF\xFF\xFF\x7F", 2199023255551),
    (b"\xFF\xFF\xFF\xFF\xFF\x7F", -2199023255552),
    (b"\x80\x80\x80\x80\x80\x80\x01", 2199023255552),
    (b"\x81\x80\x80\x80\x80\x80\x01", -2199023255553),
    (b"\xFE\xFF\xFF\xFF\xFF\xFF\x7F", 281474976710655),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\x7F", -281474976710656),
    (b"\x80\x80\x80\x80\x80\x80\x80\x01", 281474976710656),
    (b"\x81\x80\x80\x80\x80\x80\x80\x01", -281474976710657),
    (b"\xFE\xFF\xFF\xFF\xFF\xFF\xFF\x7F", 36028797018963967),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F", -36028797018963968),
    (b"\x80\x80\x80\x80\x80\x80\x80\x80\x01", 36028797018963968),
    (b"\x81\x80\x80\x80\x80\x80\x80\x80\x01", -36028797018963969),
    (b"\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F", 4611686018427387903),
    (b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F", -4611686018427387904),
    (b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01", 4611686018427387904),
    (b"\x81\x80\x80\x80\x80\x80\x80\x80\x80\x01", -4611686018427387905),
]


BENCH_VALUES_ENC = [
    60,  # 1 byte
    -8192,  # 2 bytes
    1048575,  # 3 bytes
    134217727,  # 4 bytes
    -17179869184,  # 5 bytes
    2199023255551,  # 6 bytes
]

BENCH_VALUES_DEC = [
    b"\x7E",  # 1 byte
    b"\xFF\x7F",  # 2 bytes
    b"\xFE\xFF\x7F",  # 3 bytes
    b"\xFF\xFF\xFF\x7F",  # 4 bytes
    b"\x80\x80\x80\x80\x01",  # 5 bytes
    b"\xFE\xFF\xFF\xFF\xFF\x7F",  # 6 bytes
]
BENCH_VALUES_DEC = list(map(bytearray, BENCH_VALUES_DEC))


def _assert_valid_enc(enc_func):
    for encoded, decoded in test_data:
        assert enc_func(decoded) == encoded, decoded


def _assert_valid_dec(dec_func):
    for encoded, decoded in test_data:
        res, pos = dec_func(bytearray(encoded))
        assert res == decoded, (decoded, res)
        assert pos == len(encoded), (decoded, pos)


def _assert_valid_size(size_func):
    for encoded, decoded in test_data:
        assert size_func(decoded) == len(encoded), decoded


def encode_varint_1(num):
    """ Encode an integer to a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            num (int): Value to encode

        Returns:
            bytearray: Encoded presentation of integer with length from 1 to 10
                 bytes
    """
    # Shift sign to the end of number
    num = (num << 1) ^ (num >> 63)
    # Max 10 bytes. We assert those are allocated
    buf = bytearray(10)

    for i in range(10):
        # 7 lowest bits from the number and set 8th if we still have pending
        # bits left to encode
        buf[i] = num & 0x7f | (0x80 if num > 0x7f else 0)
        num = num >> 7
        if num == 0:
            break
    else:
        # Max size of endcoded double is 10 bytes for unsigned values
        raise ValueError("Out of double range")
    return buf[:i + 1]


def encode_varint_2(value, int2byte=six.int2byte):
    value = (value << 1) ^ (value >> 63)

    bits = value & 0x7f
    value >>= 7
    res = b""
    while value:
        res += int2byte(0x80 | bits)
        bits = value & 0x7f
        value >>= 7
    return res + int2byte(bits)


def encode_varint_3(value, buf):
    append = buf.append
    value = (value << 1) ^ (value >> 63)

    bits = value & 0x7f
    value >>= 7
    while value:
        append(0x80 | bits)
        bits = value & 0x7f
        value >>= 7
    append(bits)
    return value


def encode_varint_4(value, int2byte=six.int2byte):
    value = (value << 1) ^ (value >> 63)

    if value <= 0x7f:  # 1 byte
        return int2byte(value)
    if value <= 0x3fff:  # 2 bytes
        return int2byte(0x80 | (value & 0x7f)) + int2byte(value >> 7)
    if value <= 0x1fffff:  # 3 bytes
        return int2byte(0x80 | (value & 0x7f)) + \
            int2byte(0x80 | ((value >> 7) & 0x7f)) + \
            int2byte(value >> 14)
    if value <= 0xfffffff:  # 4 bytes
        return int2byte(0x80 | (value & 0x7f)) + \
            int2byte(0x80 | ((value >> 7) & 0x7f)) + \
            int2byte(0x80 | ((value >> 14) & 0x7f)) + \
            int2byte(value >> 21)
    if value <= 0x7ffffffff:  # 5 bytes
        return int2byte(0x80 | (value & 0x7f)) + \
            int2byte(0x80 | ((value >> 7) & 0x7f)) + \
            int2byte(0x80 | ((value >> 14) & 0x7f)) + \
            int2byte(0x80 | ((value >> 21) & 0x7f)) + \
            int2byte(value >> 28)
    else:
        # Return to general algorithm
        bits = value & 0x7f
        value >>= 7
        res = b""
        while value:
            res += int2byte(0x80 | bits)
            bits = value & 0x7f
            value >>= 7
        return res + int2byte(bits)


def encode_varint_5(value, buf, pos=0):
    value = (value << 1) ^ (value >> 63)

    bits = value & 0x7f
    value >>= 7
    while value:
        buf[pos] = 0x80 | bits
        bits = value & 0x7f
        value >>= 7
        pos += 1
    buf[pos] = bits
    return pos + 1

def encode_varint_6(value, buf):
    append = buf.append
    value = (value << 1) ^ (value >> 63)

    if value <= 0x7f:  # 1 byte
        append(value)
        return 1
    if value <= 0x3fff:  # 2 bytes
        append(0x80 | (value & 0x7f))
        append(value >> 7)
        return 2
    if value <= 0x1fffff:  # 3 bytes
        append(0x80 | (value & 0x7f))
        append(0x80 | ((value >> 7) & 0x7f))
        append(value >> 14)
        return 3
    if value <= 0xfffffff:  # 4 bytes
        append(0x80 | (value & 0x7f))
        append(0x80 | ((value >> 7) & 0x7f))
        append(0x80 | ((value >> 14) & 0x7f))
        append(value >> 21)
        return 4
    if value <= 0x7ffffffff:  # 5 bytes
        append(0x80 | (value & 0x7f))
        append(0x80 | ((value >> 7) & 0x7f))
        append(0x80 | ((value >> 14) & 0x7f))
        append(0x80 | ((value >> 21) & 0x7f))
        append(value >> 28)
        return 5
    else:
        # Return to general algorithm
        bits = value & 0x7f
        value >>= 7
        i = 0
        while value:
            append(0x80 | bits)
            bits = value & 0x7f
            value >>= 7
            i += 1
    append(bits)
    return i


def size_of_varint_1(value):
    """ Number of bytes needed to encode an integer in variable-length format.
    """
    value = (value << 1) ^ (value >> 63)
    res = 0
    while True:
        res += 1
        value = value >> 7
        if value == 0:
            break
    return res


def size_of_varint_2(value):
    """ Number of bytes needed to encode an integer in variable-length format.
    """
    value = (value << 1) ^ (value >> 63)
    if value <= 0x7f:
        return 1
    if value <= 0x3fff:
        return 2
    if value <= 0x1fffff:
        return 3
    if value <= 0xfffffff:
        return 4
    if value <= 0x7ffffffff:
        return 5
    if value <= 0x3ffffffffff:
        return 6
    if value <= 0x1ffffffffffff:
        return 7
    if value <= 0xffffffffffffff:
        return 8
    if value <= 0x7fffffffffffffff:
        return 9
    return 10


if six.PY3:
    def _read_byte(memview, pos):
        """ Read a byte from memoryview as an integer

            Raises:
                IndexError: if position is out of bounds
        """
        return memview[pos]
else:
    def _read_byte(memview, pos):
        """ Read a byte from memoryview as an integer

            Raises:
                IndexError: if position is out of bounds
        """
        return ord(memview[pos])


def decode_varint_1(buffer, pos=0):
    """ Decode an integer from a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            buffer (bytes-like): any object acceptable by ``memoryview``
            pos (int): optional position to read from

        Returns:
            (int, int): Decoded int value and next read position
    """
    value = 0
    shift = 0
    memview = memoryview(buffer)
    for i in range(pos, pos + 10):
        try:
            byte = _read_byte(memview, i)
        except IndexError:
            raise ValueError("End of byte stream")
        if byte & 0x80 != 0:
            value |= (byte & 0x7f) << shift
            shift += 7
        else:
            value |= byte << shift
            break
    else:
        # Max size of endcoded double is 10 bytes for unsigned values
        raise ValueError("Out of double range")
    # Normalize sign
    return (value >> 1) ^ -(value & 1), i + 1


def decode_varint_2(buffer, pos=0):
    result = 0
    shift = 0
    while 1:
        b = buffer[pos]
        result |= ((b & 0x7f) << shift)
        pos += 1
        if not (b & 0x80):
            # result = result_type(() & mask)
            return ((result >> 1) ^ -(result & 1), pos)
        shift += 7
        if shift >= 64:
            raise ValueError("Out of int64 range")


def decode_varint_3(buffer, pos=0):
    result = buffer[pos]
    if not (result & 0x81):
        return (result >> 1), pos + 1
    if not (result & 0x80):
        return (result >> 1) ^ (~0), pos + 1

    result &= 0x7f
    pos += 1
    shift = 7
    while 1:
        b = buffer[pos]
        result |= ((b & 0x7f) << shift)
        pos += 1
        if not (b & 0x80):
            return ((result >> 1) ^ -(result & 1), pos)
        shift += 7
        if shift >= 64:
            raise ValueError("Out of int64 range")


if __name__ == '__main__':
    _assert_valid_enc(encode_varint_1)
    _assert_valid_enc(encode_varint_2)

    for encoded, decoded in test_data:
        res = bytearray()
        encode_varint_3(decoded, res)
        assert res == encoded

    _assert_valid_enc(encode_varint_4)

    # import dis
    # dis.dis(encode_varint_4)

    for encoded, decoded in test_data:
        res = bytearray(10)
        written = encode_varint_5(decoded, res)
        assert res[:written] == encoded

    for encoded, decoded in test_data:
        res = bytearray()
        encode_varint_6(decoded, res)
        assert res == encoded

    _assert_valid_size(size_of_varint_1)
    _assert_valid_size(size_of_varint_2)
    _assert_valid_dec(decode_varint_1)
    _assert_valid_dec(decode_varint_2)
    _assert_valid_dec(decode_varint_3)

    # import dis
    # dis.dis(decode_varint_3)

    runner = pyperf.Runner()
    # Encode algorithms returning a bytes result
    for bench_func in [
            encode_varint_1,
            encode_varint_2,
            encode_varint_4]:
        for i, value in enumerate(BENCH_VALUES_ENC):
            runner.bench_func(
                '{}_{}byte'.format(bench_func.__name__, i + 1),
                bench_func, value)

    # Encode algorithms writing to the buffer
    for bench_func in [
            encode_varint_3,
            encode_varint_5,
            encode_varint_6]:
        for i, value in enumerate(BENCH_VALUES_ENC):
            fname = bench_func.__name__
            runner.timeit(
                '{}_{}byte'.format(fname, i + 1),
                stmt="{}({}, buffer)".format(fname, value),
                setup="from __main__ import {}; buffer = bytearray(10)".format(
                    fname)
            )

    # Size algorithms
    for bench_func in [
            size_of_varint_1,
            size_of_varint_2]:
        for i, value in enumerate(BENCH_VALUES_ENC):
            runner.bench_func(
                '{}_{}byte'.format(bench_func.__name__, i + 1),
                bench_func, value)

    # Decode algorithms
    for bench_func in [
            decode_varint_1,
            decode_varint_2,
            decode_varint_3]:
        for i, value in enumerate(BENCH_VALUES_DEC):
            runner.bench_func(
                '{}_{}byte'.format(bench_func.__name__, i + 1),
                bench_func, value)
