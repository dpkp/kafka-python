from hashlib import sha256
from string import digits, ascii_letters

ALPHA_NUMERIC_AND_SOME_MISC = "".join(['_', '-', '~', '.']) + digits + ascii_letters


def is_alpha_numeric_or_some_misc(arg: str) -> bool:
    return arg in ALPHA_NUMERIC_AND_SOME_MISC


def bin_to_hex(s: str) -> str:
    as_bytes = s.encode()
    return as_bytes.hex().upper()


def aws_uri_encode(arg: str, encode_slash: bool = True) -> str:
    result = ''
    chars = arg

    for char in chars:
        is_alpha_numeric = is_alpha_numeric_or_some_misc(arg)
        is_slash = char == '/'
        if is_alpha_numeric:
            result += char
        elif is_slash:
            if encode_slash:
                result += '%2F'
            else:
                result += char
        else:
            result += '%' + bin_to_hex(char)

    return result


def get_digester():
    return sha256()

Ø
