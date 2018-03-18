# coding: utf-8

from thriftpy.utils import serialize as thriftpy_serialize
from thriftpy.utils import deserialize as thriftpy_deserialize
from thriftpy.protocol import TBinaryProtocol
from thriftpy.thrift import args2kwargs


class TProtocolFactory(object):

    def __init__(self, protocol_cls):
        """Initialize TProtocolFactory

        :param protocol_cls: Thrift protocol class.
        """
        self.protocol_cls = protocol_cls

    def get_protocol(self, trans):
        return self.protocol_cls(trans)


def serialize(thrift_object, protocol_cls=TBinaryProtocol) -> bytes:
    return thriftpy_serialize(thrift_object, TProtocolFactory(protocol_cls))


def deserialize(thrift_object, buf: bytes, protocol_cls=TBinaryProtocol):
    return thriftpy_deserialize(thrift_object, buf, TProtocolFactory(protocol_cls))


def convert_args_to_kwargs(thrift_args_object, *args, **kwargs):
    return {
        **args2kwargs(thrift_args_object.thrift_spec, *args),
        **kwargs
    }


def get_call_args(thrift_args_object) -> list:
    """From a thrift args object get call positional arguments"""
    thrift_spec = thrift_args_object.thrift_spec
    return [
        getattr(thrift_args_object, thrift_spec[key][1])
        for key in sorted(thrift_spec)
    ]


def get_call_result(thrift_result_object):
    if getattr(thrift_result_object, 'success', None) is not None:
        return thrift_result_object.success
    for k, v in thrift_result_object.__dict__.items():
        if k != 'success' and v:
            raise v
    return None
