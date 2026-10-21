import asyncio
import signal
import sys

from proxy.config import load_config
from proxy.logger import configure_logging
from proxy.proxy_server import ProxyServer


async def main():
    config_file_path = None
    if len(sys.argv) > 1:
        config_file_path = sys.argv[1]
    server_config = load_config(config_file_path)
    logger = configure_logging(server_config["logging"]["level"].upper())
    proxy_server = ProxyServer(server_config)
    shutdown_requested = asyncio.Event()
    event_loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        event_loop.add_signal_handler(shutdown_signal, shutdown_requested.set)
    try:
        await proxy_server.start_listening()
        await shutdown_requested.wait()
    finally:
        await proxy_server.shutdown()
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            event_loop.remove_signal_handler(shutdown_signal)
        logger.info(
            "Metrics: requests=%s errors=%s timeouts=%s active_connections=%s",
            proxy_server.metrics.requests_total, proxy_server.metrics.errors_total,
            proxy_server.metrics.timeouts_total, proxy_server.metrics.active_client_connections,
        )


if __name__ == "__main__":
    asyncio.run(main())
