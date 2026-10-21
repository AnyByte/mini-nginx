import asyncio
import logging

from proxy.client_handler import ClientConnectionHandler
from proxy.metrics import Metrics
from proxy.upstream_pool import UpstreamPool
from proxy.utils.http import BUFFER_LIMIT


class ProxyServer:
    def __init__(self, server_config):
        self.server_config = server_config
        self.upstream_pool = UpstreamPool(server_config)
        self.metrics = Metrics()
        self.tcp_server = None
        self.listen_address = None
        self.client_connection_tasks = set()
        self.is_shutting_down = False

    async def start_listening(self):
        listen_host, listen_port = self.server_config["listen"].rsplit(":", 1)
        self.tcp_server = await asyncio.start_server(
            self.on_client_connected, listen_host, int(listen_port), limit=BUFFER_LIMIT
        )
        self.listen_address = self.tcp_server.sockets[0].getsockname()
        logging.info("Listening on %s", self.listen_address)

    def on_client_connected(self, client_reader, client_writer):
        if self.is_shutting_down or len(self.client_connection_tasks) >= self.server_config["limits"]["max_client_conns"]:
            client_writer.close()
            return
        self.metrics.active_client_connections += 1
        logging.info("Получен запрос на подключение от %s", client_writer.get_extra_info("peername"))
        client_connection_handler = ClientConnectionHandler(
            client_reader=client_reader,
            client_writer=client_writer,
            upstream_pool=self.upstream_pool,
            metrics=self.metrics,
            connect_timeout_ms=self.server_config["timeouts"]["connect_ms"],
            read_timeout_ms=self.server_config["timeouts"]["read_ms"],
            write_timeout_ms=self.server_config["timeouts"]["write_ms"],
            total_timeout_ms=self.server_config["timeouts"]["total_ms"],
        )
        client_connection_task = asyncio.create_task(client_connection_handler.handle_client_connection())
        self.client_connection_tasks.add(client_connection_task)
        client_connection_task.add_done_callback(self.on_client_connection_finished)

    def on_client_connection_finished(self, client_connection_task):
        self.client_connection_tasks.remove(client_connection_task)
        self.metrics.active_client_connections -= 1
        if not client_connection_task.cancelled():
            connection_error = client_connection_task.exception()
            if connection_error:
                logging.error("Ошибка обработчика: %s", connection_error)

    async def shutdown(self, wait_for_clients_seconds=2):
        self.is_shutting_down = True
        if self.tcp_server:
            self.tcp_server.close()
        connection_tasks_to_wait_for = list(self.client_connection_tasks)
        if connection_tasks_to_wait_for:
            _, unfinished_connection_tasks = await asyncio.wait(
                connection_tasks_to_wait_for, timeout=wait_for_clients_seconds
            )
            for client_connection_task in unfinished_connection_tasks:
                client_connection_task.cancel()
            await asyncio.gather(*connection_tasks_to_wait_for, return_exceptions=True)
        if self.tcp_server:
            await self.tcp_server.wait_closed()
