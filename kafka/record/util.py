import binascii


def calc_crc32(memview):
    """ Calculate simple CRC-32 checksum over a memoryview of data
    """
    crc = binascii.crc32(memview) & 0xffffffff
    return crc
