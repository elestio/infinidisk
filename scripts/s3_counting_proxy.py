#!/usr/bin/env python3
"""Bounded, loopback-only S3 measurement proxy; never a production endpoint.

Dependencies: aiohttp (tested 3.14.3), botocore (tested 1.43.111). The latter
provides the S3-specific SigV4 signer, including preservation of encoded paths.
Install dependencies in an isolated environment, not during a benchmark.

Each backend attempt has started/forwarded/finished JSONL records. ``forwarded``
means HTTP headers were handed to the backend transport; a broken connection
does not prove server receipt or billing. No URL, object key, query value,
header, response body, exception text, or credential is written to this log.
The proxy neither retries nor follows redirects. Client retries are separate
attempts. Bodies are spooled to unnamed temporary files with strict limits.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import contextlib
import datetime
import hashlib
import hmac
import ipaddress
import json
import os
import pathlib
import re
import secrets
import tempfile
import time
import uuid
import urllib.parse
import xml.etree.ElementTree as ET

import aiohttp
from aiohttp import web
from multidict import CIMultiDict
from yarl import URL

CHUNK = 256 * 1024
HOP_HEADERS = frozenset({"connection", "proxy-connection", "keep-alive", "te",
                         "trailer", "transfer-encoding", "upgrade",
                         "proxy-authenticate", "proxy-authorization"})
SIGNING_HEADERS = frozenset({"authorization", "host", "date", "x-amz-date",
                             "x-amz-security-token", "x-amz-content-sha256",
                             "content-length", "expect"})
QUERY_KEYS = frozenset({"list-type", "prefix", "delimiter", "continuation-token",
                       "start-after", "max-keys", "encoding-type", "uploads",
                       "uploadId", "partNumber", "delete", "versionId",
                       "versions", "location", "tagging", "acl", "metadata",
                       "x-id", "marker", "key-marker", "upload-id-marker",
                       "max-uploads", "part-number-marker", "max-parts"})
ERROR_CODES = frozenset({"AccessDenied", "NoSuchKey", "NoSuchBucket",
                        "NoSuchUpload", "PreconditionFailed", "NotModified",
                        "SlowDown", "InternalError", "ServiceUnavailable",
                        "RequestTimeout", "RequestTimeTooSkewed",
                        "SignatureDoesNotMatch", "InvalidAccessKeyId",
                        "InvalidArgument", "InvalidRequest", "BadDigest",
                        "InvalidPart", "InvalidPartOrder", "EntityTooSmall",
                        "OperationAborted", "PermanentRedirect"})
PHASE_RE = re.compile(r"[a-z][a-z0-9_.-]{0,79}\Z")


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def object_type(raw_path):
    """Deliberately coarse; no pathname can escape this fixed vocabulary."""
    pieces = urllib.parse.unquote(urllib.parse.urlsplit(raw_path).path).split("/")
    if pieces[-1:] == ["HEAD"]:
        return "infinidisk_head"
    if "index" in pieces or "indexes" in pieces:
        return "infinidisk_index"
    if "segments" in pieces:
        return "segment"
    if "wal" in pieces:
        return "wal"
    if "manifest" in pieces or (pieces and pieces[-1].endswith(".manifest")):
        return "manifest"
    if "compacted" in pieces or (pieces and pieces[-1].endswith(".sst")):
        return "sst"
    return "other"


def classify_fallback(method, raw_path, headers=None):
    """Transport self-tests need no pricing dependency or network access."""
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(raw_path).query,
                                 keep_blank_values=True)
    bucket = len(urllib.parse.urlsplit(raw_path).path.strip("/").split("/")) == 1
    headers = {k.lower(): v for k, v in (headers or {}).items()}
    if bucket and method == "GET" and "location" in query:
        operation = "GetBucketLocation"
    elif "uploadId" in query:
        operation = {"PUT": "UploadPart", "POST": "CompleteMultipartUpload",
                     "DELETE": "AbortMultipartUpload", "GET": "ListParts"}.get(method, "Unknown")
        if method == "PUT" and "x-amz-copy-source" in headers:
            operation = "UploadPartCopy"
    elif "uploads" in query:
        operation = "CreateMultipartUpload" if method == "POST" else "ListMultipartUploads"
    elif method == "POST" and "delete" in query:
        operation = "DeleteObjects"
    elif method == "PUT" and "x-amz-copy-source" in headers:
        operation = "CopyObject"
    else:
        operation = {("GET", False): "GetObject", ("HEAD", False): "HeadObject",
                     ("PUT", False): "PutObject", ("DELETE", False): "DeleteObject",
                     ("GET", True): "ListObjectsV2" if "list-type" in query else "ListObjects",
                     ("HEAD", True): "HeadBucket"}.get((method, bucket), "Unknown")
    return {"operation": operation}


class EvidenceLimit(RuntimeError):
    pass


class Journal:
    """Synchronous small appends: records cannot be silently dropped by a queue."""
    def __init__(self, path, max_bytes):
        self.path = pathlib.Path(path)
        self.file = self.path.open("x", encoding="utf-8", buffering=1)
        os.chmod(self.path, 0o600)
        self.max_bytes = max_bytes
        self.bytes = 0
        self.failed = False

    def write(self, event):
        data = json.dumps({"schema_version": 1, **event}, separators=(",", ":")) + "\n"
        size = len(data.encode())
        if self.failed or self.bytes + size > self.max_bytes:
            self.failed = True
            raise EvidenceLimit("proxy evidence limit reached")
        try:
            self.file.write(data)
            self.file.flush()
            self.bytes += size
        except OSError:
            self.failed = True
            raise EvidenceLimit("proxy evidence write failed") from None

    def close(self):
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()


class CountingProxy:
    """One fixture/bucket/prefix per instance; all mutable state is loop-local."""
    def __init__(self, *, endpoint, bucket, prefix, region, credentials,
                 journal_path, scratch, classify=classify_fallback,
                 max_inflight=32, max_body_bytes=256 * 1024**2,
                 max_log_bytes=128 * 1024**2, max_requests=200_000,
                 request_timeout=180, allow_test_http=False):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/"):
            raise ValueError("backend endpoint must be an origin without credentials")
        test_http = (allow_test_http and parsed.scheme == "http" and
                     parsed.hostname in ("127.0.0.1", "::1"))
        if parsed.scheme != "https" and not test_http:
            raise ValueError("backend requires verified HTTPS")
        if not parsed.hostname or not re.fullmatch(r"[A-Za-z0-9._-]{1,63}", bucket):
            raise ValueError("invalid backend origin or bucket")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./-]{0,199}", prefix) or ".." in prefix.split("/"):
            raise ValueError("invalid dedicated prefix")
        if not (1 <= max_inflight <= 128 and 1 <= max_body_bytes <= 1024**3 and
                1 <= max_requests <= 1_000_000 and 1024 <= max_log_bytes <= 1024**3):
            raise ValueError("proxy bound outside allowed range")
        from botocore.auth import S3SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials
        self._signer_class, self._request_class = S3SigV4Auth, AWSRequest
        self._credentials = Credentials(credentials["AWS_ACCESS_KEY_ID"],
                                        credentials["AWS_SECRET_ACCESS_KEY"],
                                        credentials.get("AWS_SESSION_TOKEN"))
        self.endpoint, self.region = endpoint.rstrip("/"), region
        self.bucket, self.prefix = bucket, prefix.rstrip("/")
        self.classify = classify
        self.scratch = pathlib.Path(scratch)
        self.scratch.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.journal = Journal(journal_path, max_log_bytes)
        self.max_inflight, self.max_body_bytes = max_inflight, max_body_bytes
        self.max_requests, self.request_timeout = max_requests, request_timeout
        self.phase, self.accepting = "init", True
        self.instance_id = uuid.uuid4().hex
        self.active, self.peak, self.next_id = 0, 0, 0
        self.closed_error = None
        self.local_rejections = 0
        self.attempted, self.forwarded, self.finished = 0, 0, 0
        self.upload_bytes, self.download_bytes = 0, 0
        self.tasks = set()
        self.session = self.runner = None
        self.client_key = "measurement-" + secrets.token_hex(12)
        self.client_secret = secrets.token_hex(32)

    def client_environment(self):
        return {"AWS_ACCESS_KEY_ID": self.client_key,
                "AWS_SECRET_ACCESS_KEY": self.client_secret,
                "AWS_ALLOW_HTTP": "true", "AWS_EC2_METADATA_DISABLED": "true"}

    def _record(self, event):
        try:
            self.journal.write(event)
        except EvidenceLimit:
            self.closed_error = "evidence_limit_or_write_failure"
            self.accepting = False
            raise

    def set_phase(self, phase):
        if not PHASE_RE.fullmatch(phase):
            raise ValueError("invalid phase label")
        self._record({"event": "phase", "phase": phase, "utc": utc(),
                      "inflight_at_boundary": self.active})
        self.phase = phase

    async def start(self):
        trace = aiohttp.TraceConfig()

        async def sent(_session, context, _params):
            event = context.trace_request_ctx
            if event["forwarded"]:
                # A hidden transport retry would invalidate exact counts.
                self.closed_error = "unexpected_transport_retry"
                self.accepting = False
                raise RuntimeError("unexpected transport retry")
            event["forwarded"] = True
            self.forwarded += 1
            self._record({"event": "request_forwarded", "id": event["id"],
                          "phase": event["phase"], "utc": utc()})

        trace.on_request_headers_sent.append(sent)
        connector = aiohttp.TCPConnector(limit=self.max_inflight,
                                         limit_per_host=self.max_inflight)
        self.session = aiohttp.ClientSession(
            connector=connector, auto_decompress=False, trust_env=False,
            timeout=aiohttp.ClientTimeout(total=self.request_timeout),
            skip_auto_headers={"Accept-Encoding", "Content-Type", "User-Agent"},
            trace_configs=[trace])
        if not hasattr(self.session, "_retry_connection"):
            await self.session.close()
            raise RuntimeError("aiohttp version lacks explicit retry control")
        self.session._retry_connection = False
        app = web.Application(client_max_size=self.max_body_bytes)
        app.router.add_route("*", "/{tail:.*}", self.handle)
        self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=10)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.listen = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        self._record({"event": "proxy_started", "utc": utc(),
                      "max_inflight": self.max_inflight, "max_body_bytes": self.max_body_bytes,
                      "max_requests": self.max_requests, "max_log_bytes": self.journal.max_bytes,
                      "tls_backend_verified": self.endpoint.startswith("https://"),
                      "internal_retries": False, "redirects": False})
        return self

    def _scope(self, request):
        parsed = urllib.parse.urlsplit(request.raw_path)
        if parsed.scheme or parsed.netloc or parsed.fragment:
            return False
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True,
                                     max_num_fields=100)
        if any(key.lower().startswith("x-amz-") for key in query):
            return False  # presigned requests need separate query signing
        path = urllib.parse.unquote(parsed.path)
        if ".." in path.split("/") or "\\" in path or "\x00" in path:
            return False
        root = "/" + self.bucket
        scope = root + "/" + self.prefix
        if path in (root, root + "/"):
            if request.method == "HEAD":
                return True
            if request.method == "POST" and "delete" in query:
                return True  # validate every XML key after receiving the body
            if request.method != "GET":
                return False
            if "location" in query and set(query) <= {"location", "x-id"}:
                return True  # read-only metadata for the single allowed bucket
            prefixes = query.get("prefix", [])
            if len(prefixes) != 1 or not (prefixes[0] == self.prefix or
                                         prefixes[0].startswith(self.prefix + "/")):
                return False
        elif not (path == scope or path.startswith(scope + "/")):
            return False
        if copy_source := request.headers.get("x-amz-copy-source"):
            copy_path = urllib.parse.unquote(urllib.parse.urlsplit(copy_source).path)
            if not (copy_path == scope or copy_path.startswith(scope + "/")):
                return False
        return True

    def _metadata(self, request):
        parsed = urllib.parse.urlsplit(request.raw_path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True,
                                     max_num_fields=100)
        classified = self.classify(request.method, request.raw_path, dict(request.headers))
        operation = classified if isinstance(classified, str) else classified.get("operation", "Unknown")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", operation):
            raise ValueError("classifier returned an unsafe operation")
        scope = "bucket" if len(urllib.parse.unquote(parsed.path).strip("/").split("/")) == 1 else "object"
        range_match = re.fullmatch(r"bytes=([0-9]{1,20})-([0-9]{0,20})", request.headers.get("Range", ""))
        byte_range = ([int(range_match[1]), int(range_match[2]) if range_match[2] else None]
                      if range_match else None)
        return {"method": request.method if request.method in {"GET", "HEAD", "PUT", "POST", "DELETE"} else "OTHER",
                "operation": operation, "object_type": object_type(request.raw_path),
                "scope": scope, "range": byte_range,
                "query_keys": sorted({key if key in QUERY_KEYS else "other" for key in query})}

    def _sign(self, method, raw_path, headers, body, length):
        connection_tokens = {part.strip().lower() for part in headers.get("Connection", "").split(",")}
        blocked = HOP_HEADERS | SIGNING_HEADERS | connection_tokens
        forwarded = CIMultiDict((key, value) for key, value in headers.items()
                                if key.lower() not in blocked)
        forwarded["Content-Length"] = str(length)
        forwarded["Host"] = urllib.parse.urlsplit(self.endpoint).netloc
        req = self._request_class(method=method, url=self.endpoint + raw_path,
                                  headers=dict(forwarded), data=body)
        req.context["payload_signing_enabled"] = True
        self._signer_class(self._credentials, "s3", self.region).add_auth(req)
        body.seek(0)
        return CIMultiDict(req.headers.items())

    async def handle(self, request):
        response = None
        event = None
        task = asyncio.current_task()
        admitted = False
        body = None
        error_prefix = bytearray()
        try:
            peer = request.transport.get_extra_info("peername") if request.transport else None
            if not peer or not ipaddress.ip_address(peer[0]).is_loopback:
                raise web.HTTPForbidden(text="measurement proxy scope denied")
            credential = re.search(r"(?:^|[ ,])Credential=([^/ ,]+)/", request.headers.get("Authorization", ""))
            if not credential or not hmac.compare_digest(credential[1], self.client_key):
                raise web.HTTPForbidden(text="measurement proxy authentication required")
            if not self.accepting or self.journal.failed:
                raise web.HTTPServiceUnavailable(text="measurement proxy closed")
            if self.active >= self.max_inflight or self.next_id >= self.max_requests:
                self.closed_error = "admission_or_request_limit"
                self.accepting = False
                raise web.HTTPServiceUnavailable(text="measurement proxy bound exceeded")
            if not self._scope(request):
                raise web.HTTPForbidden(text="measurement proxy scope denied")
            if ("aws-chunked" in request.headers.get("Content-Encoding", "").lower() or
                    request.headers.get("X-Amz-Content-SHA256", "").startswith("STREAMING-")):
                raise web.HTTPBadRequest(text="AWS chunk signatures are unsupported")
            if request.content_length is not None and request.content_length > self.max_body_bytes:
                raise web.HTTPRequestEntityTooLarge(max_size=self.max_body_bytes,
                                                    actual_size=request.content_length)
            self.next_id += 1
            event = {"event": "attempt_finished", "id": self.instance_id + ":" + str(self.next_id),
                     "phase": self.phase, "started_utc": utc(), **self._metadata(request),
                     "backend_attempted": False, "forwarded": False, "status": None,
                     "transport_error": None, "proxy_error": None, "error_code": None,
                     "upload_bytes": 0, "download_bytes": 0, "completed": False}
            self.active += 1
            self.peak = max(self.peak, self.active)
            admitted = True
            self.tasks.add(task)
            started = time.monotonic()
            self._record({**event, "event": "attempt_started"})
            body = tempfile.TemporaryFile(mode="w+b", dir=self.scratch)
            length = 0
            async for chunk in request.content.iter_chunked(CHUNK):
                length += len(chunk)
                if length > self.max_body_bytes:
                    raise web.HTTPRequestEntityTooLarge(max_size=self.max_body_bytes, actual_size=length)
                await asyncio.to_thread(body.write, chunk)
            if request.method == "POST" and "delete" in request.query:
                if length > 4 * 1024**2:
                    raise web.HTTPBadRequest(text="delete XML exceeds measurement bound")
                body.seek(0)
                xml = body.read()
                if b"<!DOCTYPE" in xml.upper() or b"<!ENTITY" in xml.upper():
                    raise web.HTTPBadRequest(text="unsupported delete XML")
                try:
                    keys = [node.text or "" for node in ET.fromstring(xml).iter()
                            if node.tag.split("}")[-1] == "Key"]
                except ET.ParseError:
                    raise web.HTTPBadRequest(text="invalid delete XML") from None
                if not keys or len(keys) > 1000 or any(not (key == self.prefix or
                        key.startswith(self.prefix + "/")) for key in keys):
                    raise web.HTTPForbidden(text="delete key outside measurement prefix")
            body.seek(0)
            signed = await asyncio.to_thread(self._sign, request.method, request.raw_path,
                                             request.headers, body, length)

            async def upload():
                while chunk := await asyncio.to_thread(body.read, CHUNK):
                    event["upload_bytes"] += len(chunk)
                    yield chunk

            event["backend_attempted"] = True
            self.attempted += 1
            async with self.session.request(request.method,
                    URL(self.endpoint + request.raw_path, encoded=True), headers=signed,
                    data=upload() if length else b"", allow_redirects=False,
                    trace_request_ctx=event) as upstream:
                event["status"] = upstream.status
                conn = {part.strip().lower() for part in upstream.headers.get("Connection", "").split(",")}
                headers = CIMultiDict((k, v) for k, v in upstream.headers.items()
                                      if k.lower() not in HOP_HEADERS | conn)
                response = web.StreamResponse(status=upstream.status, headers=headers)
                await response.prepare(request)
                async for chunk in upstream.content.iter_chunked(CHUNK):
                    event["download_bytes"] += len(chunk)
                    if upstream.status >= 400 and len(error_prefix) < 8192:
                        error_prefix.extend(chunk[:8192 - len(error_prefix)])
                    await response.write(chunk)
                await response.write_eof()
                event["completed"] = True
                return response
        except web.HTTPException as error:
            self.local_rejections += 1
            if event is not None:
                event["proxy_error"] = "local_rejection"
            return error
        except asyncio.CancelledError:
            if event is not None:
                event["transport_error"] = "cancelled"
            raise
        except (TimeoutError, asyncio.TimeoutError):
            if event is not None:
                event["transport_error"] = "timeout"
        except aiohttp.ClientConnectorError:
            if event is not None:
                event["transport_error"] = "connection"
        except (aiohttp.ClientError, ConnectionError):
            if event is not None:
                event["transport_error"] = "stream_or_connection"
        except EvidenceLimit:
            if event is not None:
                event["proxy_error"] = "evidence_limit_or_write_failure"
        except Exception:
            self.closed_error = "proxy_internal_failure"
            self.accepting = False
            if event is not None:
                event["proxy_error"] = "proxy_internal_failure"
        finally:
            if event is not None and admitted:
                event["seconds"] = time.monotonic() - started
                if error_prefix:
                    match = re.search(rb"<Code>([A-Za-z0-9]+)</Code>", error_prefix)
                    if match:
                        code = match[1].decode("ascii")
                        event["error_code"] = code if code in ERROR_CODES else "Other"
                with contextlib.suppress(EvidenceLimit):
                    self._record(event)
                self.finished += 1
                self.upload_bytes += event["upload_bytes"]
                self.download_bytes += event["download_bytes"]
            if body is not None:
                body.close()
            if admitted:
                self.active -= 1
                self.tasks.discard(task)
        if response is not None and response.prepared:
            if request.transport:
                request.transport.close()
            return response
        return web.Response(status=502, text="measurement backend request failed")

    def snapshot(self):
        return {"backend_attempts": self.attempted, "forwarded": self.forwarded,
                "finished": self.finished, "inflight": self.active, "peak_inflight": self.peak,
                "upload_bytes": self.upload_bytes, "download_bytes": self.download_bytes,
                "local_rejections": self.local_rejections, "closed_error": self.closed_error,
                "journal_complete": not self.journal.failed}

    async def close(self):
        self.accepting = False
        # Call only after our clients have exited. Existing attempts are drained,
        # with a timeout/cancellation recorded, before closing their file handles.
        if self.tasks:
            done, pending = await asyncio.wait(self.tasks, timeout=self.request_timeout + 2)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
                self.closed_error = self.closed_error or "shutdown_cancelled_inflight"
        if self.runner is not None:
            await self.runner.cleanup()
        if self.session is not None:
            await self.session.close()
        with contextlib.suppress(EvidenceLimit):
            self._record({"event": "proxy_stopped", "utc": utc(), **self.snapshot()})
        self.journal.close()


def completed_events(path):
    with pathlib.Path(path).open() as source:
        for line in source:
            event = json.loads(line)
            if event.get("event") == "attempt_finished":
                yield event


def consolidated_events(path):
    """One row per admitted attempt, including an interrupted partial request."""
    events = {}
    for line in pathlib.Path(path).read_text().splitlines():
        item = json.loads(line)
        kind, identity = item.get("event"), item.get("id")
        if kind == "attempt_started":
            if identity in events:
                raise ValueError("duplicate proxy attempt identity")
            events[identity] = dict(item)
        elif kind == "request_forwarded":
            if identity not in events or events[identity]["forwarded"]:
                raise ValueError("unmatched or duplicate forwarded event")
            events[identity]["forwarded"] = True
        elif kind == "attempt_finished":
            if identity not in events or events[identity].get("event") == "attempt_finished":
                raise ValueError("unmatched or duplicate finished event")
            if events[identity]["forwarded"] != item["forwarded"]:
                raise ValueError("forwarded state mismatch")
            events[identity] = item
    for item in events.values():
        if item["event"] != "attempt_finished":
            item.update(event="attempt_incomplete", completed=False,
                        transport_error="interrupted_without_final_record")
    return list(events.values())


async def self_test():
    """All upstream traffic is loopback; real credentials are never inspected."""
    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials
    fake_key, fake_secret, fake_token = "test-access-DO-NOT-LOG", "test-secret-DO-NOT-LOG", "test-token-DO-NOT-LOG"
    received = []
    counters = collections.Counter()
    slow_gate = asyncio.Event()
    active, peak = 0, 0

    async def backend(request):
        nonlocal active, peak
        data = await request.read()
        raw = request.raw_path
        auth = request.headers.get("Authorization", "")
        assert fake_key in auth and request.headers.get("X-Amz-Security-Token") == fake_token
        assert request.headers["X-Amz-Content-SHA256"] == hashlib.sha256(data).hexdigest()
        # Verify independently from received headers, preserving their timestamp.
        timestamp = request.headers["X-Amz-Date"]
        signed_names = re.search(r"SignedHeaders=([^, ]+)", auth)[1].split(";")
        verify = AWSRequest(method=request.method,
                            url="http://" + request.host + raw,
                            data=data, headers={key: request.headers[key] for key in signed_names})
        verify.context["timestamp"] = timestamp
        signer = S3SigV4Auth(Credentials(fake_key, fake_secret, fake_token), "s3", "test-region")
        canonical = signer.canonical_request(verify)
        expected = signer.signature(signer.string_to_sign(verify, canonical), verify)
        assert auth.endswith("Signature=" + expected)
        received.append((request.method, raw, len(data)))
        counters[raw] += 1
        active += 1
        peak = max(peak, active)
        try:
            if raw.endswith("/slow"):
                await slow_gate.wait()
            if raw.endswith("/retry") and counters[raw] == 1:
                return web.Response(status=503, body=b"<Error><Code>SlowDown</Code></Error>")
            if raw.endswith("/missing"):
                return web.Response(status=404, body=b"<Error><Code>NoSuchKey</Code></Error>")
            if request.method == "HEAD":
                return web.Response(headers={"Content-Length": "17", "ETag": '"opaque"'})
            return web.Response(body=b"backend-payload", status=206 if "Range" in request.headers else 200)
        finally:
            active -= 1

    with tempfile.TemporaryDirectory(prefix="s3-proxy-selftest-") as temp:
        directory = pathlib.Path(temp)
        app = web.Application(client_max_size=1024 * 1024)
        app.router.add_route("*", "/{tail:.*}", backend)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        endpoint = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        proxy = CountingProxy(endpoint=endpoint, bucket="bucket", prefix="fixture",
                              region="test-region", credentials={"AWS_ACCESS_KEY_ID": fake_key,
                                "AWS_SECRET_ACCESS_KEY": fake_secret, "AWS_SESSION_TOKEN": fake_token},
                              journal_path=directory / "events.jsonl", scratch=directory / "spool",
                              max_inflight=4, max_body_bytes=64 * 1024,
                              allow_test_http=True)
        await proxy.start()
        headers = {"Authorization": "AWS4-HMAC-SHA256 Credential=" + proxy.client_key + "/date/test/s3/aws4_request, Signature=unused"}
        try:
            async with aiohttp.ClientSession() as client:
                async def call(method, suffix, **kwargs):
                    async with client.request(method, proxy.listen + "/bucket" + suffix,
                                              headers={**headers, **kwargs.pop("headers", {})}, **kwargs) as response:
                        await response.read()
                        return response.status
                assert await call("GET", "/fixture/retry") == 503
                assert len(received) == 1  # proxy never retries the 503
                assert await call("GET", "/fixture/retry") == 200
                proxy.set_phase("load")
                for method, suffix, body in [
                    ("HEAD", "/fixture/a", None),
                    ("GET", "?location", None),
                    ("GET", "?list-type=2&prefix=fixture%2F&continuation-token=QUERY-SECRET", None),
                    ("POST", "/fixture/a?uploads", b""),
                    ("PUT", "/fixture/a?partNumber=1&uploadId=UPLOAD-SECRET", b"x" * 1024),
                    ("POST", "/fixture/a?uploadId=UPLOAD-SECRET", b"<CompleteMultipartUpload/>"),
                    ("DELETE", "/fixture/a?uploadId=UPLOAD-SECRET", None),
                    ("DELETE", "/fixture/a", None),
                    ("POST", "?delete", b"<Delete><Object><Key>fixture/a</Key></Object></Delete>"),
                    ("GET", "/fixture/missing", None),
                    ("PUT", "/fixture/a%20b", b"raw payload"),
                ]:
                    assert await call(method, suffix, data=body) in (200, 404)
                assert await call("GET", "/fixture/range", headers={"Range": "bytes=4-19"}) == 206
                proxy.set_phase("concurrent")
                requests = [asyncio.create_task(call("GET", "/fixture/slow")) for _ in range(4)]
                for _ in range(100):
                    if active == 4:
                        break
                    await asyncio.sleep(.01)
                assert active == 4 and peak == 4
                slow_gate.set()
                assert await asyncio.gather(*requests) == [200] * 4
                count = len(received)
                assert await call("PUT", "/other/a", data=b"forbidden") == 403
                assert await call("GET", "?list-type=2") == 403
                assert await call("GET", "?acl") == 403
                assert await call("GET", "?location&list-type=2") == 403
                assert await call("POST", "?delete", data=b"<Delete><Object><Key>production/a</Key></Object></Delete>") == 403
                assert await call("PUT", "/fixture/huge", data=b"x" * (64 * 1024 + 1)) == 413
                assert len(received) == count
        finally:
            await proxy.close()
            await runner.cleanup()
        events = list(completed_events(directory / "events.jsonl"))
        assert consolidated_events(directory / "events.jsonl") == events
        forwarded = [event for event in events if event["forwarded"]]
        assert len(forwarded) == len(received) == proxy.forwarded == proxy.attempted
        assert sum(event["upload_bytes"] for event in forwarded) == sum(length for _, _, length in received)
        assert {event["operation"] for event in forwarded} >= {"HeadObject", "GetBucketLocation", "ListObjectsV2", "UploadPart", "CompleteMultipartUpload", "AbortMultipartUpload", "DeleteObject", "DeleteObjects"}
        assert any(event["error_code"] == "SlowDown" for event in events)
        assert any(event["range"] == [4, 19] for event in events)
        assert proxy.active == 0 and not list((directory / "spool").iterdir())
        text = (directory / "events.jsonl").read_text()
        for private in (fake_key, fake_secret, fake_token, proxy.client_key, proxy.client_secret,
                        "QUERY-SECRET", "UPLOAD-SECRET", "Authorization", "backend-payload"):
            assert private not in text
        # Explicit bound must fail closed, without a silently partial journal.
        journal = Journal(directory / "bounded.jsonl", 16)
        try:
            try:
                journal.write({"event": "larger_than_limit"})
            except EvidenceLimit:
                pass
            else:
                raise AssertionError("journal did not enforce limit")
            assert journal.failed
        finally:
            journal.close()
        partial = directory / "partial.jsonl"
        partial.write_text(json.dumps({"event": "attempt_started", "id": "partial:1",
            "forwarded": False, "operation": "GetObject", "status": None}) + "\n" +
            json.dumps({"event": "request_forwarded", "id": "partial:1"}) + "\n")
        remaining = consolidated_events(partial)
        assert len(remaining) == 1 and remaining[0]["forwarded"]
        assert remaining[0]["transport_error"] == "interrupted_without_final_record"
        print(json.dumps({"self_test": "passed", "backend_requests": len(received),
                          "exact_counts": True, "sigv4_verified": True,
                          "max_concurrent": peak, "secret_redaction": True,
                          "scope_and_body_bounds": True, "spools_removed": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", required=True)
    parser.parse_args()
    asyncio.run(self_test())
