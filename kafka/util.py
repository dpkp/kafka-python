from collections import defaultdict
from itertools import groupby
import struct

def write_int_string(s):
    if s is None:
        return struct.pack('>i', 0) # TODO change this to -1 when KAFKA-771 is accepted
    else:
        return struct.pack('>i%ds' % len(s), len(s), s)

def write_short_string(s):
    if s is None:
        return struct.pack('>h', 0) # TODO change this to -1 when KAFKA-771 is accepted
    else:
        return struct.pack('>h%ds' % len(s), len(s), s)

def read_short_string(data, cur):
    if len(data) < cur+2:
        raise BufferUnderflowError("Not enough data left")
    (strLen,) = struct.unpack('>h', data[cur:cur+2])
    if strLen == -1:
        return (None, cur+2)
    cur += 2
    if len(data) < cur+strLen:
        raise BufferUnderflowError("Not enough data left")
    out = data[cur:cur+strLen]
    return (out, cur+strLen)

def read_int_string(data, cur):
    if len(data) < cur+4:
        raise BufferUnderflowError("Not enough data left")
    (strLen,) = struct.unpack('>i', data[cur:cur+4])
    if strLen == -1:
        return (None, cur+4)
    cur += 4
    if len(data) < cur+strLen:
        raise BufferUnderflowError("Not enough data left")
    out = data[cur:cur+strLen]
    return (out, cur+strLen)

def relative_unpack(fmt, data, cur):
    size = struct.calcsize(fmt)
    if len(data) < cur+size:
        raise BufferUnderflowError("Not enough data left")
    out = struct.unpack(fmt, data[cur:cur+size])
    return (out, cur+size)

def group_by_topic_and_partition(tuples):
    out = defaultdict(dict)
    for t in tuples:
        out[t.topic][t.partition] = t
    return out 

class BufferUnderflowError(Exception):
    pass

class ChecksumError(Exception):
    pass
