from typing import Any
from datetime import datetime, timezone
import socket
import ssl
import httpx


def type_in_cause_chain(exc, theType):
    if isinstance(exc, theType):
        return True
    if exc.__cause__ and type_in_cause_chain(exc.__cause__, theType):
        return True
    if exc.__context__ and type_in_cause_chain(exc.__context__, theType):
        return True
    return False


def classify_exception(exc: Exception) -> str:
    if type_in_cause_chain(exc, socket.gaierror):
        return "dns_error"
    elif type_in_cause_chain(exc, ssl.SSLError):
        return "tls_error"
    elif isinstance(exc, httpx.ConnectError) or isinstance(exc, httpx.ConnectTimeout):
        return "connect_error"
    elif isinstance(exc, httpx.TimeoutException):
        return "timeout"
    elif isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code == 404 or exc.response.status_code == 410:
            return "nodeinfo_missing"
        else:
            return "http_error"
    elif isinstance(exc, ValueError):
        return "nodeinfo_invalid"
    return "error"


class HostSurveyor:

    def __init__(self, client):
        self.client = client

    async def survey(self, hostname: str) -> dict[str, Any]:
        props = {"last_fetch_date": datetime.now(timezone.utc).isoformat()}
        try:
            fetched = await self.client.get_nodeinfo(hostname)
            if fetched is None:
                props["failure"] = "nodeinfo_invalid"
                props["error_detail"] = "No data returned from get_nodeinfo()"
            else:
              filtered = {k: v for k, v in fetched.items() if v is not None}
              props = {**filtered, **props}
        except Exception as exc:
            props["failure"] = classify_exception(exc)
            props["error_detail"] = repr(exc)[:500]
        return props
