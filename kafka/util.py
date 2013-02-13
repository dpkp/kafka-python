import struct

def write_int_string(s):
    return struct.pack('>i%ds' % len(s), len(s), s)

def write_short_string(s):
    return struct.pack('>H%ds' % len(s), len(s), s)

def read_short_string(data, cur):
    if len(data) < cur+2:
        raise IOError("Not enough data left")
    (strLen,) = struct.unpack('>H', data[cur:cur+2])
    if strLen == -1:
        return (None, cur+2)
    cur += 2
    if len(data) < cur+strLen:
        raise IOError("Not enough data left")
    out = data[cur:cur+strLen]
    return (out, cur+strLen)

def read_int_string(data, cur):
    if len(data) < cur+4:
        raise IOError("Not enough data left")
    (strLen,) = struct.unpack('>i', data[cur:cur+4])
    if strLen == -1:
        return (None, cur+4)
    cur += 4
    if len(data) < cur+strLen:
        raise IOError("Not enough data left")
    out = data[cur:cur+strLen]
    return (out, cur+strLen)

def relative_unpack(fmt, data, cur):
    size = struct.calcsize(fmt)
    if len(data) < cur+size:
        raise IOError("Not enough data left")
    out = struct.unpack(fmt, data[cur:cur+size])
    return (out, cur+size)
