# coding: utf-8

from typing import Dict

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.TSerialization import (
    serialize as thrift_serialize,
    deserialize as thrift_deserialize,
)


class TProtocolFactory(object):

    def __init__(self, protocol_cls):
        """Initialize TProtocolFactory

        :param protocol_cls: Thrift protocol class.
        """
        self.protocol_cls = protocol_cls

    def get_protocol(self, trans):
        return self.protocol_cls(trans)

    getProtocol = get_protocol


class ThriftService(object):

    def __init__(self, service_module, handler=None):
        self.name = service_module.__name__.split('.')[-1]
        self.service_module = service_module
        self.handler = handler
        self._service_rpc_map: Dict[str, tuple] = {}
        for rpc_name in service_module.Iface.__dict__:
            rpc_args = getattr(service_module, rpc_name + '_args', None)
            rpc_result = getattr(service_module, rpc_name + '_result', None)
            if rpc_args is None or rpc_result is None:
                continue
            self._service_rpc_map[rpc_name] = (rpc_args, rpc_result)

    def has_rpc(self, rpc_name: str) -> bool:
        return rpc_name in self._service_rpc_map

    def get_rpc_args_and_result_object(self, rpc_name: str) -> tuple:
        rpc_args, rpc_result = self._service_rpc_map[rpc_name]
        return rpc_args(), rpc_result()


def serialize(thrift_object, protocol_cls=TBinaryProtocol) -> bytes:
    return thrift_serialize(thrift_object, TProtocolFactory(protocol_cls))


def deserialize(thrift_object, buf: bytes, protocol_cls=TBinaryProtocol):
    return thrift_deserialize(thrift_object, buf, TProtocolFactory(protocol_cls))


def convert_args_to_kwargs(thrift_args_object, *args, **kwargs):
    arg_names = [_[2] for _ in thrift_args_object.thrift_spec if _]
    return {**dict(zip(arg_names, args)), **kwargs}


def get_call_args(thrift_args_object) -> list:
    """From a thrift args object get call positional arguments"""
    arg_names = [_[2] for _ in thrift_args_object.thrift_spec if _]
    return [getattr(thrift_args_object, arg_name) for arg_name in arg_names]


def get_call_result(thrift_result_object):
    if getattr(thrift_result_object, 'success', None) is not None:
        return thrift_result_object.success
    for k, v in thrift_result_object.__dict__.items():
        if k != 'success' and v:
            raise v
    return None
