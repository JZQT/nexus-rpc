# coding: utf-8

from inspect import iscoroutine

from thriftpy.protocol.binary import TBinaryProtocol
from aiohttp.web import Application, Request, Response, run_app
from aiohttp.web_exceptions import HTTPNotFound, HTTPInternalServerError

from nexus.platform.thrift import serialize, deserialize, get_call_args


class AsyncNexusServer(object):

    def __init__(self, services_map: list, address: tuple, protocol_cls=TBinaryProtocol):
        """Initialize AsyncNexusServer

        :param services_map: A list of (thrift_service, api_handler) two-tuples.
        :param address: A (host, port) tuple.
        :param protocol_cls: Thrift protocol class, default is `TBinaryProtocol`.
        """
        self.services_map = {_[0].__name__: _ for _ in services_map}
        self.address = address
        self.protocol_cls = protocol_cls
        self._app = Application()
        self._app.router.add_post('/{service}/{api}', self._handle_request)

    @staticmethod
    async def _process(rpc_impl, call_args):
        ret = rpc_impl(*call_args)
        if iscoroutine(ret):
            return await ret
        return ret

    async def _handle_request(self, request: Request):
        service_name = request.match_info['service']
        api_name = request.match_info['api']

        if service_name not in self.services_map:
            raise HTTPNotFound(body=b'')

        service_class, handler = self.services_map[service_name]
        if api_name not in service_class.thrift_services:
            raise HTTPNotFound(body=b'')

        rpc_impl = getattr(handler, api_name)
        rpc_args = getattr(service_class, api_name + '_args')()
        rpc_result = getattr(service_class, api_name + '_result')()
        rpc_result_thrift_spec = rpc_result.thrift_spec

        deserialize(rpc_args, await request.read(), self.protocol_cls)

        call_args = get_call_args(rpc_args)

        try:
            rpc_result.success = await self._process(rpc_impl, call_args)
        except Exception as e:
            for key in sorted(rpc_result_thrift_spec):
                _, exc_name, exc_class, *_ = rpc_result_thrift_spec[key]
                if exc_name == 'success':
                    continue
                if isinstance(e, exc_class):
                    setattr(rpc_result, exc_name, e)
                    break
            else:
                raise HTTPInternalServerError(body=b'') from e

        if rpc_result.oneway:
            return Response(body=b'')

        return Response(body=serialize(rpc_result, self.protocol_cls))

    def run(self):
        run_app(self._app, host=self.address[0], port=self.address[1])
