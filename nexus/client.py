# coding: utf-8

from functools import partial

from aiohttp import ClientSession
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.thrift import TApplicationException

from .platform.thrift import (
    serialize, deserialize, convert_args_to_kwargs, get_call_result
)


class AsyncNexusClient(object):

    def __init__(self, services: list, address: tuple, timeout=None, protocol_cls=TBinaryProtocol):
        """Initialize AsyncNexusClient

        :param services: A list of thrift service object.
        :param address: A (host, port) tuple.
        :param timeout: Timeout seconds.
        :param protocol_cls: Thrift protocol class, default is `TBinaryProtocol`.
        """
        self.address = address
        self.protocol_cls = protocol_cls
        self.timeout = timeout
        self.httpclient = ClientSession()
        self.services = {
            service.__name__: _AsyncNexusClientService(service, self)
            for service in services
        }

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

    def __init__(self, service, client: AsyncNexusClient):
        self.service = service
        self.client = client

    def __getattr__(self, item: str):
        if item not in self.service.thrift_services:
            raise AttributeError(item)
        return partial(self._call, item)

    async def _call(self, api_name: str, *args, **kwargs):
        url = 'http://{}:{}/{}/{}'.format(*self.client.address, self.service.__name__, api_name)
        rpc_args = getattr(self.service, api_name + '_args')()
        rpc_result = getattr(self.service, api_name + '_result')()

        for k, v in convert_args_to_kwargs(rpc_args, *args, **kwargs).items():
            setattr(rpc_args, k, v)

        data = serialize(rpc_args, self.client.protocol_cls)

        async with self.client.httpclient.post(url, data=data, timeout=self.client.timeout) as resp:
            if resp.status == 404:
                raise TApplicationException(
                    TApplicationException.UNKNOWN_METHOD,
                    'Server RPC Method Not Found'
                )
            if resp.status == 500:
                raise TApplicationException(
                    TApplicationException.INTERNAL_ERROR, 'Internal Server Error'
                )
            assert resp.status < 400, "Unknown HTTP Error"
            deserialize(rpc_result, await resp.read(), self.client.protocol_cls)

        return get_call_result(rpc_result)
