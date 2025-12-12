import asyncio
import json
import os
from typing import Any
from urllib.parse import urlparse

import httpx
from httpx import AsyncClient, Response

from .accounts_pool import Account, AccountsPool
from .logger import logger
from .utils import utc
from .xclid import XClIdGen

ReqParams = dict[str, str | int] | None
TMP_TS = utc.now().isoformat().split(".")[0].replace("T", "_").replace(":", "-")[0:16]


class HandledError(Exception): ...


class AbortReqError(Exception): ...


class XClIdGenStore:
    items: dict[str, XClIdGen] = {}  # username -> XClIdGen

    @classmethod
    async def get(cls, username: str, fresh=False) -> XClIdGen:
        if username in cls.items and not fresh:
            return cls.items[username]

        tries = 0
        while tries < 3:
            try:
                clid_gen = await XClIdGen.create()
                cls.items[username] = clid_gen
                return clid_gen
            except httpx.HTTPStatusError:
                tries += 1
                await asyncio.sleep(1)

        raise AbortReqError(
            "Faield to create XClIdGen. See: https://github.com/vladkens/twscrape/issues/248"
        )


class Ctx:
    def __init__(self, acc: Account, clt: AsyncClient):
        self.req_count = 0
        self.acc = acc
        self.clt = clt

    async def aclose(self):
        await self.clt.aclose()

    async def req(self, method: str, url: str, params: ReqParams = None) -> Response:
        # if code 404 on first try then generate new x-client-transaction-id and retry
        # https://github.com/vladkens/twscrape/issues/248
        path = urlparse(url).path or "/"

        tries = 0
        while tries < 3:
            gen = await XClIdGenStore.get(self.acc.username, fresh=tries > 0)
            hdr = {"x-client-transaction-id": gen.calc(method, path)}
            rep = await self.clt.request(method, url, params=params, headers=hdr)
            if rep.status_code != 404:
                return rep

            tries += 1
            logger.debug(f"Retrying request with new x-client-transaction-id: {url}")
            await asyncio.sleep(1)

        raise AbortReqError(
            "Faield to get XClIdGen. See: https://github.com/vladkens/twscrape/issues/248"
        )


def req_id(rep: Response):
    lr = str(rep.headers.get("x-rate-limit-remaining", -1))
    ll = str(rep.headers.get("x-rate-limit-limit", -1))
    sz = max(len(lr), len(ll))
    lr, ll = lr.rjust(sz), ll.rjust(sz)

    username = getattr(rep, "__username", "<UNKNOWN>")
    return f"{lr}/{ll} - {username}"


def dump_rep(rep: Response):
    count = getattr(dump_rep, "__count", -1) + 1
    setattr(dump_rep, "__count", count)

    acc = getattr(rep, "__username", "<unknown>")
    outfile = f"{count:05d}_{rep.status_code}_{acc}.txt"
    outfile = f"/tmp/twscrape-{TMP_TS}/{outfile}"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    msg = []
    msg.append(f"{count:,d} - {req_id(rep)}")
    msg.append(f"{rep.status_code} {rep.request.method} {rep.request.url}")
    msg.append("\n")
    # msg.append("\n".join([str(x) for x in list(rep.request.headers.items())]))
    msg.append("\n".join([str(x) for x in list(rep.headers.items())]))
    msg.append("\n")

    try:
        msg.append(json.dumps(rep.json(), indent=2))
    except json.JSONDecodeError:
        msg.append(rep.text)

    txt = "\n".join(msg)
    with open(outfile, "w") as f:
        f.write(txt)


class QueueClient:
    # Track soft errors per account for pattern detection
    _soft_error_counts: dict[str, int] = {}  # username -> consecutive soft error count
    _soft_error_threshold = 5  # Lock after this many consecutive soft errors
    _soft_error_lock_minutes = 3  # Lock duration for soft errors

    # Exponential backoff for error 88 (rate limit exceeded with remaining > 0)
    _ban_strikes: dict[str, int] = {}  # username -> strike count
    _ban_strike_max = 6  # Mark inactive after this many strikes (after ~24hr total backoff)
    _ban_backoff_base_minutes = 60  # Base backoff: 60, 120, 240, 480, 540 min (~24hr total), then inactive

    def __init__(self, pool: AccountsPool, queue: str, debug=False, proxy: str | None = None):
        self.pool = pool
        self.queue = queue
        self.debug = debug
        self.ctx: Ctx | None = None
        self.proxy = proxy

    async def __aenter__(self):
        await self._get_ctx()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close_ctx()

    async def _close_ctx(self, reset_at=-1, inactive=False, msg: str | None = None):
        if self.ctx is None:
            return

        ctx, self.ctx, self.req_count = self.ctx, None, 0
        username = ctx.acc.username
        await ctx.aclose()

        if inactive:
            await self.pool.mark_inactive(username, msg)
            return

        if reset_at > 0:
            await self.pool.lock_until(ctx.acc.username, self.queue, reset_at, ctx.req_count)
            return

        await self.pool.unlock(ctx.acc.username, self.queue, ctx.req_count)

    async def _get_ctx(self):
        if self.ctx:
            return self.ctx

        acc = await self.pool.get_for_queue_or_wait(self.queue)
        if acc is None:
            return None

        clt = acc.make_client(proxy=self.proxy)
        self.ctx = Ctx(acc, clt)
        return self.ctx

    async def _check_rep(self, rep: Response) -> None:
        """
        This function can raise Exception and request will be retried or aborted
        Or if None is returned, response will passed to api parser as is
        """

        if self.debug:
            dump_rep(rep)

        try:
            res = rep.json()
        except json.JSONDecodeError:
            res: Any = {"_raw": rep.text}

        limit_remaining = int(rep.headers.get("x-rate-limit-remaining", -1))
        limit_reset = int(rep.headers.get("x-rate-limit-reset", -1))
        # limit_max = int(rep.headers.get("x-rate-limit-limit", -1))

        err_msg = "OK"
        if "errors" in res:
            err_msg = set([f"({x.get('code', -1)}) {x['message']}" for x in res["errors"]])
            err_msg = "; ".join(list(err_msg))

        log_msg = f"{rep.status_code:3d} - {req_id(rep)} - {err_msg}"
        logger.trace(log_msg)

        # for dev: need to add some features in api.py
        if err_msg.startswith("(336) The following features cannot be null"):
            logger.error(f"[DEV] Update required: {err_msg}")
            exit(1)

        # general api rate limit
        if limit_remaining == 0 and limit_reset > 0:
            logger.debug(f"Rate limited: {log_msg}")
            # Log the raw response details for analysis
            self._log_rate_limit_response(rep, err_msg, "normal_rate_limit")
            await self._close_ctx(limit_reset)
            raise HandledError()

        # Error 88 with remaining > 0 indicates possible ban - use exponential backoff
        if err_msg.startswith("(88) Rate limit exceeded") and limit_remaining > 0:
            self._log_rate_limit_response(rep, err_msg, "error_88_ban")
            await self._handle_ban_strike(err_msg)
            raise HandledError()

        if err_msg.startswith("(326) Authorization: Denied by access control"):
            logger.warning(f"Ban detected: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg.startswith("(32) Could not authenticate you"):
            logger.warning(f"Session expired or banned: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg == "OK" and rep.status_code == 403:
            logger.warning(f"Session expired or banned: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=None)
            raise HandledError()

        # something from twitter side - abort all queries, see: https://github.com/vladkens/twscrape/pull/80
        if err_msg.startswith("(131) Dependency: Internal error"):
            # looks like when data exists, we can ignore this error
            # https://github.com/vladkens/twscrape/issues/166
            if rep.status_code == 200 and "data" in res and "user" in res["data"]:
                err_msg = "OK"
            else:
                logger.warning(f"Dependency error (request skipped): {err_msg}")
                raise AbortReqError()

        # content not found
        if rep.status_code == 200 and "_Missing: No status found with that ID" in err_msg:
            return  # ignore this error

        # something from twitter side - just ignore it, see: https://github.com/vladkens/twscrape/pull/95
        if rep.status_code == 200 and "Authorization" in err_msg:
            logger.warning(f"Authorization unknown error: {log_msg}")
            return

        if err_msg != "OK":
            logger.warning(f"API unknown error: {log_msg}")
            # Log unknown API errors for analysis
            self._log_rate_limit_response(rep, err_msg, "api_unknown_error")

            # Handle soft errors with pattern detection
            await self._handle_soft_error(err_msg)
            return  # continue after handling

        # Reset soft error counter on success
        if self.ctx:
            self._reset_soft_errors(self.ctx.acc.username)

        # Log successful requests (sampled to reduce volume)
        # Sample 1 in 20 successful requests, or always log if remaining is low
        import random
        if limit_remaining < 20 or random.randint(1, 20) == 1:
            self._log_rate_limit_response(rep, "OK", "success")

        try:
            rep.raise_for_status()
        except httpx.HTTPStatusError:
            logger.error(f"Unhandled API response code: {log_msg}")
            await self._close_ctx(utc.ts() + 60 * 15)  # 15 minutes
            raise HandledError()

    def _reset_soft_errors(self, username: str):
        """Reset soft error counter for an account after successful request."""
        if username in QueueClient._soft_error_counts:
            del QueueClient._soft_error_counts[username]
        # Also reset ban strikes on successful request
        if username in QueueClient._ban_strikes:
            del QueueClient._ban_strikes[username]

    async def _handle_ban_strike(self, err_msg: str):
        """
        Handle error 88 (rate limit exceeded with remaining > 0) with exponential backoff.

        Strike 1: 15 min backoff
        Strike 2: 30 min backoff
        Strike 3: 60 min backoff
        Strike 4: 120 min backoff
        Strike 5: Mark inactive
        """
        if self.ctx is None:
            return

        username = self.ctx.acc.username
        QueueClient._ban_strikes[username] = QueueClient._ban_strikes.get(username, 0) + 1
        strikes = QueueClient._ban_strikes[username]

        if strikes >= QueueClient._ban_strike_max:
            logger.warning(
                f"Account {username} hit {strikes} ban strikes, marking inactive"
            )
            del QueueClient._ban_strikes[username]
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            return

        # Exponential backoff: 60, 120, 240, 480, then remainder to reach 24hr total
        # Total: 60 + 120 + 240 + 480 + 540 = 1440 min (24hr)
        base_backoff = QueueClient._ban_backoff_base_minutes * (2 ** (strikes - 1))
        cumulative_so_far = sum(60 * (2 ** i) for i in range(strikes - 1))  # Previous backoffs
        remaining_to_24hr = 1440 - cumulative_so_far
        backoff_minutes = min(base_backoff, remaining_to_24hr)
        logger.warning(
            f"Account {username} ban strike {strikes}/{QueueClient._ban_strike_max}, "
            f"backing off for {backoff_minutes} minutes"
        )
        await self._close_ctx(utc.ts() + 60 * backoff_minutes)

    async def _handle_soft_error(self, err_msg: str):
        """
        Handle soft errors (200 status with error in body) with pattern detection.

        - (29) Timeout errors: Lock immediately for 2 minutes (Twitter overloaded)
        - (0) Not found: Ignore single occurrences (could be deleted content)
        - Other errors: Track consecutive occurrences, lock after threshold
        """
        if self.ctx is None:
            return

        username = self.ctx.acc.username

        # Immediate lock for timeout errors - Twitter is telling us to slow down
        if "(29) Timeout" in err_msg:
            logger.info(f"Timeout error for {username}, locking for 2 minutes")
            await self._close_ctx(utc.ts() + 60 * 2)  # 2 minutes
            raise HandledError()

        # For other soft errors, track consecutive occurrences
        QueueClient._soft_error_counts[username] = QueueClient._soft_error_counts.get(username, 0) + 1
        count = QueueClient._soft_error_counts[username]

        if count >= QueueClient._soft_error_threshold:
            logger.warning(
                f"Account {username} hit {count} consecutive soft errors, "
                f"locking for {QueueClient._soft_error_lock_minutes} minutes"
            )
            QueueClient._soft_error_counts[username] = 0  # Reset counter
            await self._close_ctx(utc.ts() + 60 * QueueClient._soft_error_lock_minutes)
            raise HandledError()

    def _log_rate_limit_response(self, rep: Response, err_msg: str, event_type: str):
        """Log rate limit response details to a file for analysis"""
        import json
        from datetime import datetime
        from pathlib import Path

        # Create logs directory if it doesn't exist
        log_dir = Path.home() / ".twscrape" / "rate_limit_logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Use a rotating log file (one per day)
        log_file = log_dir / f"rate_limits_{datetime.now().strftime('%Y%m%d')}.jsonl"

        # Parse response body to get raw errors
        try:
            response_body = rep.json()
        except:
            response_body = {"_raw": rep.text[:500]}  # Truncate if too long

        # Extract raw errors array from response
        raw_errors = response_body.get("errors", [])

        # Parse URL to extract endpoint info
        url_str = str(rep.url)
        endpoint_name = "unknown"
        if "/graphql/" in url_str:
            parts = url_str.split("/graphql/")[1].split("/")
            if len(parts) >= 2:
                # Get just the operation name, remove query params
                endpoint_name = parts[1].split("?")[0]

        # Get all response headers
        all_response_headers = dict(rep.headers)

        # Get request details from the response object
        request_details = {
            "method": rep.request.method if hasattr(rep, 'request') else "N/A",
            "url": str(rep.request.url) if hasattr(rep, 'request') else url_str,
        }

        # Get request headers we sent
        request_headers = {}
        if hasattr(rep, 'request') and hasattr(rep.request, 'headers'):
            request_headers = dict(rep.request.headers)
            # Extract key headers for summary
            request_details["user_agent"] = request_headers.get("user-agent", "N/A")
            request_details["authorization"] = request_headers.get("authorization", "N/A")[:50] + "..." if request_headers.get("authorization") else "N/A"
            request_details["x_csrf_token"] = request_headers.get("x-csrf-token", "N/A")
            request_details["x_client_transaction_id"] = request_headers.get("x-client-transaction-id", "N/A")
            request_details["content_type"] = request_headers.get("content-type", "N/A")

        # Extract rate limit headers
        rate_limit_data = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "account": getattr(rep, "__username", "UNKNOWN"),
            "status_code": rep.status_code,
            "endpoint": endpoint_name,
            "url": url_str,

            # Request details (what we sent)
            "request": {
                "method": request_details.get("method", "N/A"),
                "user_agent": request_details.get("user_agent", "N/A"),
                "authorization_type": "Bearer" if "Bearer" in request_details.get("authorization", "") else "N/A",
                "x_csrf_token": request_details.get("x_csrf_token", "N/A"),
                "x_client_transaction_id": request_details.get("x_client_transaction_id", "N/A"),
                "content_type": request_details.get("content_type", "N/A"),
                "all_request_headers": request_headers,  # Full request headers
            },

            # Response details
            "error_message_formatted": err_msg,
            "raw_errors": raw_errors,  # Full error array from Twitter

            # Rate limit headers
            "rate_limit_headers": {
                "limit": rep.headers.get("x-rate-limit-limit", "N/A"),
                "remaining": rep.headers.get("x-rate-limit-remaining", "N/A"),
                "reset": rep.headers.get("x-rate-limit-reset", "N/A"),
            },

            # All response headers (for debugging)
            "response_headers": all_response_headers,

            # Response body summary (first few keys)
            "response_keys": list(response_body.keys()) if isinstance(response_body, dict) else [],
        }

        # Append to log file (JSONL format - one JSON object per line)
        try:
            with open(log_file, "a") as f:
                f.write(json.dumps(rate_limit_data) + "\n")
        except Exception as e:
            logger.debug(f"Failed to log rate limit response: {e}")

    async def get(self, url: str, params: ReqParams = None) -> Response | None:
        return await self.req("GET", url, params=params)

    async def req(self, method: str, url: str, params: ReqParams = None) -> Response | None:
        unknown_retry, connection_retry = 0, 0

        while True:
            ctx = await self._get_ctx()  # not need to close client, class implements __aexit__
            if ctx is None:
                return None

            try:
                rep = await ctx.req(method, url, params=params)
                setattr(rep, "__username", ctx.acc.username)
                await self._check_rep(rep)

                ctx.req_count += 1  # count only successful
                unknown_retry, connection_retry = 0, 0
                return rep
            except AbortReqError:
                # abort all queries
                return
            except HandledError:
                # retry with new account
                continue
            except (httpx.ReadTimeout, httpx.ProxyError):
                # http transport failed, just retry with same account
                continue
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                # if proxy missconfigured or ???
                connection_retry += 1
                if connection_retry >= 3:
                    raise e
            except Exception as e:
                unknown_retry += 1
                if unknown_retry >= 3:
                    msg = [
                        "Unknown error. Account timeouted for 15 minutes.",
                        "Create issue please: https://github.com/vladkens/twscrape/issues",
                        f"If it mistake, you can unlock accounts with `twscrape reset_locks`. Err: {type(e)}: {e}",
                    ]

                    logger.warning(" ".join(msg))
                    await self._close_ctx(utc.ts() + 60 * 15)  # 15 minutes
