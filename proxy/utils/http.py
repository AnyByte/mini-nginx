import asyncio
from http import HTTPStatus

from proxy.errors import BadRequest
from proxy.timeouts import wait_with_timeout

BUFFER_LIMIT = 64_000


def parse_headers(header_lines):
    headers = {}
    for header_line in header_lines:
        try:
            header_name, header_value = header_line.split(":", 1)
        except ValueError:
            continue
        headers[header_name.lower()] = header_value.strip()
    return headers


def parse_request_head(raw_request: bytes) -> tuple[str, str, dict, bytes, int]:
    """Разбирает стартовую строку и заголовки, возвращает начало тела и его длину"""
    header_bytes, initial_body = raw_request.split(b"\r\n\r\n", 1)
    request_line, *header_lines = header_bytes.decode("latin-1").split("\r\n")

    try:
        request_method, request_path, _ = request_line.split(" ", 2)
    except ValueError:
        raise BadRequest("Неверная стартовая строка")

    request_headers = parse_headers(header_lines)
    if "transfer-encoding" in request_headers:
        raise BadRequest("Для тела запроса нужен Content-Length")
    try:
        request_body_length = int(request_headers.get("content-length", 0))
        if request_body_length < 0:
            raise ValueError()
    except ValueError:
        raise BadRequest("Неверное значение content-length")

    return request_method, request_path, request_headers, initial_body, request_body_length


def parse_response_head(raw_response):
    header_bytes = raw_response.split(b"\r\n\r\n", 1)[0]
    status_line, *header_lines = header_bytes.decode("latin-1").split("\r\n")
    try:
        response_status = int(status_line.split(" ", 2)[1])
    except (ValueError, IndexError):
        raise BadRequest("Неверная строка ответа")
    return response_status, parse_headers(header_lines)


def build_http_response(response_status: int, response_body: bytes = b"", content_type: str = "text/html; charset=utf-8") -> bytes:
    if not response_body and response_status >= 400:
        response_body = f"<h1>{response_status} {HTTPStatus(response_status).phrase}</h1>".encode()
    response_head = (
        f"HTTP/1.1 {response_status} {HTTPStatus(response_status).phrase}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(response_body)}\r\n"
        "Connection: close\r\n"
    ).encode()
    return b"\r\n".join([response_head, response_body])


def prepare_forwarded_head(raw_head, should_close_connection):
    message_lines = raw_head[:-4].decode("latin-1").split("\r\n")
    message_headers = parse_headers(message_lines[1:])
    # Connection и перечисленные в нём заголовки относятся к одному TCP-соединению.
    headers_to_remove = ["connection", "keep-alive", "proxy-connection"]
    for header_name in message_headers.get("connection", "").split(","):
        headers_to_remove.append(header_name.strip().lower())

    forwarded_lines = [message_lines[0]]
    for header_line in message_lines[1:]:
        header_name = header_line.split(":", 1)[0].lower()
        if header_name not in headers_to_remove:
            forwarded_lines.append(header_line)
    if should_close_connection:
        forwarded_lines.append("Connection: close")
    else:
        forwarded_lines.append("Connection: keep-alive")
    return ("\r\n".join(forwarded_lines) + "\r\n\r\n").encode("latin-1")


async def write_to_connection(connection_writer, outgoing_data, write_timeout_ms):
    connection_writer.write(outgoing_data)
    await wait_with_timeout(connection_writer.drain(), timeout_ms=write_timeout_ms)


async def read_message_head(connection_reader, read_timeout_ms):
    try:
        message_head = await wait_with_timeout(
            connection_reader.readuntil(b"\r\n\r\n"), timeout_ms=read_timeout_ms
        )
    except asyncio.IncompleteReadError as error:
        if not error.partial:
            return b""
        raise BadRequest("Оборванные заголовки")
    except asyncio.LimitOverrunError:
        raise BadRequest("Запрос слишком большой")
    if len(message_head) > BUFFER_LIMIT:
        raise BadRequest("Запрос слишком большой")
    return message_head


async def forward_body(source_reader, destination_writer, remaining_body_bytes, read_timeout_ms, write_timeout_ms):
    while remaining_body_bytes is None or remaining_body_bytes > 0:
        chunk_size = BUFFER_LIMIT
        if remaining_body_bytes is not None:
            chunk_size = min(chunk_size, remaining_body_bytes)
        body_chunk = await wait_with_timeout(source_reader.read(chunk_size), timeout_ms=read_timeout_ms)
        if not body_chunk:
            if remaining_body_bytes:
                raise BadRequest("Оборванное тело запроса")
            return
        await write_to_connection(destination_writer, body_chunk, write_timeout_ms=write_timeout_ms)
        if remaining_body_bytes is not None:
            remaining_body_bytes -= len(body_chunk)


async def close_connection(connection_writer):
    connection_writer.close()
    try:
        await asyncio.wait_for(connection_writer.wait_closed(), 2)
    except (OSError, TimeoutError):
        pass
