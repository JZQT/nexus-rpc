# coding: utf-8

import logging
from typing import Dict
from inspect import iscoroutine

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from aiohttp.web import Application, Request, Response, run_app
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError

from .platform.thrift import serialize, deserialize, get_call_args, ThriftService

logger = logging.getLogger(__name__)


class AsyncNexusServer(object):

    def __init__(self, services_map: list, address: tuple, protocol_cls=TBinaryProtocol):
        """Initialize AsyncNexusServer

        :param services_map: A list of (thrift_service, api_handler) two-tuples.
        :param address: A (host, port) tuple.
        :param protocol_cls: Thrift protocol class, default is `TBinaryProtocol`.
        """
        self.services_map: Dict[str, ThriftService] = {}
        for service_module, handler in services_map:
            service = ThriftService(service_module, handler)
            self.services_map[service.name] = service
        self.address = address
        self.protocol_cls = protocol_cls
        self._app = Application()
        self._app.router.add_post('/{service}/{rpc}', self._handle_request)

    def _has_service(self, service_name: str) -> bool:
        return service_name in self.services_map

    @staticmethod
    async def _process(rpc_impl, call_args):
        ret = rpc_impl(*call_args)
        if iscoroutine(ret):
            return await ret
        return ret

    async def _handle_request(self, request: Request):
        service_name = request.match_info['service']
        rpc_name = request.match_info['rpc']

        if not self._has_service(service_name):
            raise HTTPNotFound(body=b'')

        service = self.services_map[service_name]

        if not service.has_rpc(rpc_name):
            raise HTTPNotFound(body=b'')

        rpc_impl = getattr(service.handler, rpc_name)
        rpc_args, rpc_result = service.get_rpc_args_and_result_object(rpc_name)

        deserialize(rpc_args, await request.read(), self.protocol_cls)

        call_args = get_call_args(rpc_args)

        try:
            rpc_result.success = await self._process(rpc_impl, call_args)
        except Exception as e:
            for _, _, exc_name, exc_type_info, *_ in rpc_result.thrift_spec:
                if exc_name == 'success':
                    continue
                exc_class, *_ = exc_type_info
                if isinstance(e, exc_class):
                    setattr(rpc_result, exc_name, e)
                    break
            else:
                logger.exception('NexusServiceError')
                raise HTTPInternalServerError(body=b'') from e

        return Response(body=serialize(rpc_result, self.protocol_cls))

    def run(self):
        run_app(self._app, host=self.address[0], port=self.address[1])
