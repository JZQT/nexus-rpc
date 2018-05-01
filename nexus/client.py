# coding: utf-8

from functools import partial

from aiohttp import ClientSession
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.Thrift import TApplicationException

from .platform.thrift import (
    serialize, deserialize, convert_args_to_kwargs, get_call_result, ThriftService
)


class AsyncNexusClient(object):

    def __init__(self, services: list, address: tuple, timeout=None, protocol_cls=TBinaryProtocol):
        """Initialize AsyncNexusClient

        :param services: A list of thrift service module.
        :param address: A (host, port) tuple.
        :param timeout: Timeout seconds.
        :param protocol_cls: Thrift protocol class, default is `TBinaryProtocol`.
        """
        self.address = address
        self.protocol_cls = protocol_cls
        self.timeout = timeout
        self.httpclient = ClientSession()
        self.services = {}
        for service_module in services:
            service = ThriftService(service_module)
            self.services[service.name] = _AsyncNexusClientService(service, self)

    def __getattr__(self, item: str):
        if item not in self.services:
            raise AttributeError(item)
        return self.services[item]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def close(self):
        return self.httpclient.close()


class _AsyncNexusClientService(object):

    def __init__(self, service: ThriftService, client: AsyncNexusClient):
        self.service = service
        self.client = client

    def __getattr__(self, item: str):
        if not self.service.has_rpc(item):
            raise AttributeError('service {!r} has no api {!r}'.format(self.service.name, item))
        return partial(self._call, item)

    async def _call(self, rpc_name: str, *args, **kwargs):
        service_name = self.service.name
        url = 'http://{}:{}/{}/{}'.format(*self.client.address, service_name, rpc_name)
        rpc_args, rpc_result = self.service.get_rpc_args_and_result_object(rpc_name)

        for k, v in convert_args_to_kwargs(rpc_args, *args, **kwargs).items():
            setattr(rpc_args, k, v)

        data = serialize(rpc_args, self.client.protocol_cls)

        async with self.client.httpclient.post(url, data=data, timeout=self.client.timeout) as resp:
            if resp.status == 404:
                raise TApplicationException(TApplicationException.UNKNOWN_METHOD)
            if resp.status == 500:
                raise TApplicationException(TApplicationException.INTERNAL_ERROR)
            assert resp.status < 400, "Unknown HTTP Error"
            deserialize(rpc_result, await resp.read(), self.client.protocol_cls)

        return get_call_result(rpc_result)
