# These need to get registered before ApiMessage metaclass starts building classes
from .field_basic import FieldBasicType
from .api_struct import ApiStruct
from .api_array import ApiArray
from .api_struct_array import ApiStructArray

from .api_message import ApiMessage
from .api_header import ApiHeader, RequestHeader, ResponseHeader
