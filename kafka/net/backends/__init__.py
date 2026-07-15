from .abstract import (
    NetBackend, NetTransport, NetProtocol, NetBackendFuture,
    resolve_backend, register_backend_lazy,
)

register_backend_lazy('selector', 'kafka.net.selector', 'NetworkSelector')
register_backend_lazy('asyncio', 'kafka.net.backends.asyncio_backend', 'AsyncioBackend')
