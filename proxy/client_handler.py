import asyncio
import logging
from time import time

from proxy.errors import BadRequest
from proxy.timeouts import wait_with_timeout
from proxy.utils.http import (
    BUFFER_LIMIT, close_connection, forward_body, prepare_forwarded_head, build_http_response,
    parse_request_head, parse_response_head, read_message_head, write_to_connection,
)


class ClientConnectionHandler:
    def __init__(self, client_reader, client_writer, upstream_pool, metrics,
                 connect_timeout_ms, read_timeout_ms, write_timeout_ms, total_timeout_ms):
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.upstream_pool = upstream_pool
        self.metrics = metrics
        self.connect_timeout_ms = connect_timeout_ms
        self.read_timeout_ms = read_timeout_ms
        self.write_timeout_ms = write_timeout_ms
        self.total_timeout_ms = total_timeout_ms
        self.response_status = 0
        self.response_started = False

    async def handle_client_connection(self):
        try:
            while await self.process_client_request():
                pass
        finally:
            await close_connection(self.client_writer)

    async def process_client_request(self):
        request_started_at = time()
        request_method, request_path, upstream_address = "-", "-", "-"
        self.response_status = 0
        self.response_started = False
        try:
            # Таймаут, чтобы медленный клиент не держал соединение вечно
            async with asyncio.timeout(self.total_timeout_ms / 1000):
                request_head = await read_message_head(self.client_reader, read_timeout_ms=self.read_timeout_ms)
                if not request_head:
                    return False
                request_method, request_path, request_headers, _, request_body_length = parse_request_head(request_head)
                upstream_host, upstream_port, upstream_connection_semaphore = self.upstream_pool.choose_next_upstream()
                upstream_address = f"{upstream_host}:{upstream_port}"
                self.metrics.requests_per_upstream[upstream_address] = (
                    self.metrics.requests_per_upstream.get(upstream_address, 0) + 1
                )
                logging.info("start %s %s upstream=%s", request_method, request_path, upstream_address)

                async with upstream_connection_semaphore:
                    upstream_reader, upstream_writer = await wait_with_timeout(
                        asyncio.open_connection(upstream_host, upstream_port, limit=BUFFER_LIMIT),
                        timeout_ms=self.connect_timeout_ms,
                    )
                    try:
                        await write_to_connection(
                            upstream_writer,
                            prepare_forwarded_head(request_head, should_close_connection=True),
                            write_timeout_ms=self.write_timeout_ms,
                        )
                        forward_request_task = asyncio.create_task(forward_body(
                            self.client_reader, upstream_writer, request_body_length,
                            read_timeout_ms=self.read_timeout_ms,
                            write_timeout_ms=self.write_timeout_ms,
                        ))
                        forward_response_task = asyncio.create_task(self.forward_upstream_response(
                            upstream_reader, request_headers, forward_request_task
                        ))
                        try:
                            _, keep_client_connection_open = await asyncio.gather(
                                forward_request_task, forward_response_task
                            )
                        finally:
                            forward_request_task.cancel()
                            forward_response_task.cancel()
                            await asyncio.gather(
                                forward_request_task, forward_response_task, return_exceptions=True
                            )
                    finally:
                        await close_connection(upstream_writer)
                return keep_client_connection_open
        except BadRequest as error:
            self.metrics.errors_total += 1
            logging.warning(error)
            await self.send_error_response(400)
        except TimeoutError:
            self.metrics.errors_total += 1
            self.metrics.timeouts_total += 1
            logging.warning("Timeout: %s %s upstream=%s", request_method, request_path, upstream_address)
            await self.send_error_response(504)
        except Exception as error:
            self.metrics.errors_total += 1
            logging.exception(error)
            await self.send_error_response(502)
        finally:
            if self.response_status:
                request_duration_seconds = time() - request_started_at
                self.metrics.record_completed_request(self.response_status, request_duration_seconds)
                logging.info(
                    "%s %s upstream=%s status=%s duration=%.3fs",
                    request_method, request_path, upstream_address, self.response_status, request_duration_seconds,
                )
        return False

    async def forward_upstream_response(self, upstream_reader, request_headers, forward_request_task):
        try:
            response_head = await read_message_head(upstream_reader, read_timeout_ms=self.read_timeout_ms)
            if not response_head:
                raise OSError("Upstream закрыл соединение до ответа")
            response_status, response_headers = parse_response_head(response_head)
        except BadRequest as error:
            raise OSError(str(error))

        self.response_status = response_status
        keep_client_connection_open = request_headers.get("connection", "").lower() != "close"
        if "content-length" not in response_headers and "transfer-encoding" not in response_headers:
            keep_client_connection_open = False
        # Ранний ответ: непрочитанное тело не должно стать следующим запросом.
        if not forward_request_task.done():
            keep_client_connection_open = False
        self.response_started = True
        await write_to_connection(
            self.client_writer,
            prepare_forwarded_head(response_head, should_close_connection=not keep_client_connection_open),
            write_timeout_ms=self.write_timeout_ms,
        )
        # Upstream закрывает соединение после ответа, поэтому читаем до EOF.
        await forward_body(
            upstream_reader, self.client_writer, None,
            read_timeout_ms=self.read_timeout_ms,
            write_timeout_ms=self.write_timeout_ms,
        )
        return keep_client_connection_open

    async def send_error_response(self, response_status):
        if self.response_started:
            return
        self.response_status = response_status
        try:
            await write_to_connection(
                self.client_writer, build_http_response(response_status),
                write_timeout_ms=self.write_timeout_ms,
            )
        except (OSError, TimeoutError):
            pass
