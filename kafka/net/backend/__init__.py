from .abstract import (
    NetBackend, NetTransport, NetProtocol, NetBackendFuture,
    list_backends, resolve_backend, register_backend_lazy,
)

register_backend_lazy('selector', 'kafka.net.backend.selector', 'NetworkSelector')
register_backend_lazy('asyncio', 'kafka.net.backend.asyncio_backend', 'AsyncioBackend')
