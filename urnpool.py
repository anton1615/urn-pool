#!/usr/bin/env python3
"""URNnetwork account pool wrapper.

Hands out one dedicated URnetwork exit per client.  A client asks for a node
with a name of its own choosing, the wrapper starts an urnproxy adapter
instance for it, and the client talks to a stable local port for as long as
it wants.

Design notes
------------
The wrapper never changes urnproxy (the URnetwork socks adapter from
github.com/urnetwork/proxy, seedphrase-login patched): it only starts the
binary and talks to api.bringyour.com for the quota bookkeeping.

Each client gets two ports:

  public   21000+  the tailnet address the client connects to; stable for
                   the life of the client, so a mihomo subscription that
                   points at it never goes stale.
  internal 31000+  urnproxy's own SOCKS5 listener, bound to 127.0.0.1.

A small TCP relay sits in front of the internal port.  It speaks no
protocol at all -- it just moves bytes -- because SOCKS5 carries one tunnel
per TCP connection, so each client connection is independent of every other
one.  That is what makes a swap invisible: a replacement adapter is started
and warmed up, the relay is pointed at it, and the previous instance is
retired only once the connections it already carries have drained.

Account supply
--------------
`pool.txt` holds the account seedphrases, one per line; the `auth/*.json`
files hold their metadata (`seedphrase`, `network_id`, `network_name`,
`created_at`; everything else in them is ignored).  Both are read again on
every poll cycle, so accounts added to the pool later (the register script
appends them) are picked up automatically.

Quota: every account gets a free daily grant (UTC 00:00, ~30 GiB) bound to
the network_id.  The wrapper probes /subscription/balance each cycle, moves a
client to a fresh account when its current one runs dry, and dead accounts
come back to life automatically when the next daily grant lands.

API
---
`--api-token <secret>` is optional; when given, every API path lives under
`/<secret>/` as well.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import random
import re
import socket
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_POOL = os.path.join(HERE, "pool.txt")
DEFAULT_AUTH_DIR = os.path.join(HERE, "auth")
DEFAULT_LOG_DIR = os.path.join(HERE, "logs")
DEFAULT_DB = os.path.join(HERE, "db.sqlite")
DEFAULT_URNPROXY = "/data/home/urn/urnproxy"
HUB_OPEN = os.path.join(HERE, "hub-open.json")   # urn-hub.py's open set
API_URL = "https://api.bringyour.com"
DEFAULT_COUNTRY = "United States"

PUBLIC_PORT_BASE = 21000
INTERNAL_PORT_BASE = 31000
ACCOUNTS_PER_INSTANCE = 1
ROTATE_QUOTA_FRACTION = 0.05      # rotate once even the best account is under 5%
DRAIN_TIMEOUT = 120.0             # seconds to wait for the old backend to drain
PROBE_CONCURRENCY = 4
PROBE_SAMPLE = 24                 # free 帳號每輪只探這麼多，其餘沿用上次數字
BACKEND_READY_TIMEOUT = 45.0
RESERVED_PATHS = {"register", "delete", "sub", "locations", "status",
                  "country", "rotate", "providers", "favicon.ico"}


class LoginRejected(Exception):
    """The seedphrase no longer logs in (account gone or auth removed)."""



def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def safe_name(uid: str) -> str:
    """A filesystem-safe, collision-free name for a client identifier."""
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", uid)[:40] or "client"
    return f"{stem}-{hashlib.sha1(uid.encode()).hexdigest()[:8]}"


# ── state ───────────────────────────────────────────────────────────────────

@dataclass
class Account:
    token: str                  # seedphrase
    network_id: str = ""
    network_name: str = ""
    quota_max: int = 0
    quota_left: int = 0
    quota_reset: str = ""
    state: str = "free"          # free | assigned | dead
    assigned_to: str = ""


@dataclass
class Instance:
    uid: str
    public_port: int
    internal_port: int
    country: str = ""
    proxy: str = ""
    tokens: list[str] = field(default_factory=list)
    created_at: float = 0.0
    rotated_at: float = 0.0
    backend: Any = None          # Backend
    front: Any = None            # Front


class Store:
    """SQLite state.  Only ever touched from the event loop thread."""

    def __init__(self, path: str):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                token       TEXT PRIMARY KEY,
                network_id  TEXT NOT NULL DEFAULT '',
                network_name TEXT NOT NULL DEFAULT '',
                quota_max   INTEGER NOT NULL DEFAULT 0,
                quota_left  INTEGER NOT NULL DEFAULT 0,
                quota_reset TEXT NOT NULL DEFAULT '',
                state       TEXT NOT NULL DEFAULT 'free',
                assigned_to TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS instances (
                uid           TEXT PRIMARY KEY,
                public_port   INTEGER NOT NULL UNIQUE,
                internal_port INTEGER NOT NULL,
                country       TEXT NOT NULL DEFAULT '',
                proxy         TEXT NOT NULL DEFAULT '',
                tokens        TEXT NOT NULL DEFAULT '[]',
                created_at    REAL NOT NULL DEFAULT 0,
                rotated_at    REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self.db.commit()

    def next_port(self, key: str, base: int) -> int:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        nxt = int(row["value"]) if row else base
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(nxt + 1)))
        self.db.commit()
        return nxt

    def accounts(self) -> list[Account]:
        rows = self.db.execute("SELECT * FROM accounts").fetchall()
        return [Account(token=r["token"], network_id=r["network_id"],
                        network_name=r["network_name"],
                        quota_max=r["quota_max"], quota_left=r["quota_left"],
                        quota_reset=r["quota_reset"], state=r["state"],
                        assigned_to=r["assigned_to"]) for r in rows]

    def upsert_account(self, account: Account) -> None:
        self.db.execute(
            "INSERT INTO accounts(token,network_id,network_name,quota_max,quota_left,quota_reset,state,assigned_to)"
            " VALUES(?,?,?,?,?,?,?,?)"
            " ON CONFLICT(token) DO UPDATE SET network_id=excluded.network_id, network_name=excluded.network_name",
            (account.token, account.network_id, account.network_name, account.quota_max,
             account.quota_left, account.quota_reset, account.state, account.assigned_to))
        self.db.commit()

    def set_account_state(self, token: str, state: str, assigned_to: str = "") -> None:
        self.db.execute("UPDATE accounts SET state=?, assigned_to=? WHERE token=?",
                        (state, assigned_to, token))
        self.db.commit()

    def set_account_quota(self, token: str, quota_max: int, quota_left: int,
                          quota_reset: str) -> None:
        self.db.execute(
            "UPDATE accounts SET quota_max=?, quota_left=?, quota_reset=?,"
            # a successful probe with quota left proves the account is alive, even if it was marked dead earlier
            " state=CASE WHEN ?<=0 THEN 'dead' WHEN state='dead' THEN 'free' ELSE state END WHERE token=?",
            (quota_max, quota_left, quota_reset, quota_left, token))
        self.db.commit()

    def instances(self) -> list[Instance]:
        rows = self.db.execute("SELECT * FROM instances").fetchall()
        return [Instance(uid=r["uid"], public_port=r["public_port"],
                         internal_port=r["internal_port"], country=r["country"],
                         proxy=r["proxy"], tokens=json.loads(r["tokens"]),
                         created_at=r["created_at"], rotated_at=r["rotated_at"])
                for r in rows]

    def instance(self, uid: str) -> Instance | None:
        for inst in self.instances():
            if inst.uid == uid:
                return inst
        return None

    def put_instance(self, inst: Instance) -> None:
        self.db.execute(
            "INSERT INTO instances(uid,public_port,internal_port,country,proxy,tokens,created_at,rotated_at)"
            " VALUES(?,?,?,?,?,?,?,?)"
            " ON CONFLICT(uid) DO UPDATE SET public_port=excluded.public_port,"
            " internal_port=excluded.internal_port, country=excluded.country,"
            " proxy=excluded.proxy, tokens=excluded.tokens,"
            " created_at=excluded.created_at, rotated_at=excluded.rotated_at",
            (inst.uid, inst.public_port, inst.internal_port, inst.country, inst.proxy,
             json.dumps(inst.tokens), inst.created_at, inst.rotated_at))
        self.db.commit()

    def delete_instance(self, uid: str) -> None:
        self.db.execute("DELETE FROM instances WHERE uid=?", (uid,))
        self.db.commit()


# ── TCP relay ───────────────────────────────────────────────────────────────

class Front:
    """A stable public port that relays to the current backend.

    It moves bytes and knows nothing about SOCKS5.  Each accepted
    connection is bound to the backend that was current when it arrived, so
    retiring a backend does not disturb the connections it already carries.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.backend: tuple[str, int] | None = None
        self._server: asyncio.AbstractServer | None = None
        self._relays: dict[int, tuple[str, int]] = {}
        self._next_id = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        log(f"front listening on {self.host}:{self.port}")

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()

    def point_at(self, backend: tuple[str, int]) -> None:
        self.backend = backend

    def relay_count(self, backend: tuple[str, int]) -> int:
        return sum(1 for v in self._relays.values() if v == backend)

    def total_relays(self) -> int:
        return len(self._relays)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        backend = self.backend
        if backend is None:
            writer.close()
            return
        br = bw = None
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            try:
                br, bw = await asyncio.wait_for(
                    asyncio.open_connection(*backend), timeout=3.0)
                break
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.3)
        if br is None or bw is None:
            writer.close()
            return
        self._next_id += 1
        relay_id = self._next_id
        self._relays[relay_id] = backend
        try:
            await asyncio.gather(self._pipe(reader, bw), self._pipe(br, writer),
                                 return_exceptions=True)
        finally:
            self._relays.pop(relay_id, None)
            for w in (writer, bw):
                with contextlib.suppress(Exception):
                    w.close()

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()


# ── urnproxy backend ───────────────────────────────────────────────────────

class Backend:
    """One urnproxy process serving one client."""

    def __init__(self, proc: subprocess.Popen, port: int, tokens: list[str]):
        self.proc = proc
        self.port = port
        self.tokens = tokens
        self.started_at = time.time()

    def alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self, timeout: float = 10.0) -> None:
        if not self.alive():
            return
        with contextlib.suppress(Exception):
            self.proc.terminate()
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(Exception):
                self.proc.kill()
                self.proc.wait(timeout=5)


# ── wrapper ─────────────────────────────────────────────────────────────────

class Wrapper:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.store = Store(args.db)
        self.instances: dict[str, Instance] = {}
        self.loop: asyncio.AbstractEventLoop | None = None
        self._jwts: dict[str, tuple[str, float]] = {}   # seedphrase -> (by_jwt, expiry)
        self._quota_lock = asyncio.Lock()
        self._rotate_lock = asyncio.Lock()
        self._servers: tuple[float, dict] | None = None
        self._providers: dict[str, tuple[float, list[dict]]] = {}

    # -- account supply ---------------------------------------------------

    def read_pool(self) -> list[str]:
        try:
            with open(self.args.pool, encoding="utf-8") as fh:
                return [line.strip() for line in fh
                        if line.strip() and not line.startswith("#")]
        except OSError:
            return []

    def read_metadata(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        try:
            names = os.listdir(self.args.auth_dir)
        except OSError:
            return out
        for name in names:
            if not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(self.args.auth_dir, name), encoding="utf-8") as fh:
                    rec = json.load(fh)
            except (OSError, ValueError):
                continue
            token = (rec.get("seedphrase") or "").strip()
            if token:
                out[token] = rec
        return out

    def refresh_accounts(self) -> int:
        """Pick up accounts that appeared in the pool since the last scan."""
        known = {a.token for a in self.store.accounts()}
        metadata = self.read_metadata()
        added = 0
        for token in self.read_pool():
            if token in known:
                continue
            rec = metadata.get(token, {})
            self.store.upsert_account(Account(
                token=token, network_id=rec.get("network_id", ""),
                network_name=rec.get("network_name", "")))
            added += 1
        if added:
            log(f"pool grew: {added} new account(s), {len(self.store.accounts())} total")
        return added

    # -- URnetwork quota ---------------------------------------------------

    def _login(self, seedphrase: str) -> str:
        """Seedphrase -> by_jwt, cached for the token's 30-day life."""
        cached = self._jwts.get(seedphrase)
        now = time.time()
        if cached and cached[1] > now + 300:
            return cached[0]
        body = json.dumps({"seedphrase": seedphrase}).encode()
        req = urllib.request.Request(
            API_URL + "/auth/login", data=body,
            headers={"Content-Type": "application/json"})
        try:
            resp = json.load(urllib.request.urlopen(req, timeout=30))
            jwt = resp["network"]["by_jwt"]
        except Exception:
            raise LoginRejected(seedphrase[:12])
        self._jwts[seedphrase] = (jwt, now + 29 * 86400)
        return jwt

    def _balance(self, seedphrase: str) -> tuple[int, int, str]:
        """(quota_max, quota_left, reset) for one account."""
        jwt = self._login(seedphrase)
        req = urllib.request.Request(
            API_URL + "/subscription/balance",
            headers={"Authorization": "Bearer " + jwt})
        resp = json.load(urllib.request.urlopen(req, timeout=30))
        resets = [b.get("end_time", "") for b in resp.get("active_transfer_balances", [])]
        return (int(resp.get("start_balance_byte_count") or 0),
                int(resp.get("balance_byte_count") or 0),
                max(resets) if resets else "")

    async def probe_account(self, account: Account) -> None:
        for attempt in (1, 2):
            try:
                quota_max, quota_left, quota_reset = await asyncio.to_thread(
                    self._balance, account.token)
            except LoginRejected:
                self.store.set_account_state(account.token, "dead")
                log(f"  {account.network_name or account.token[:12]}: rejected by URnetwork API")
                return
            except Exception as exc:  # noqa: BLE001
                if attempt == 1:
                    await asyncio.sleep(1.0)
                    continue
                log(f"  {account.network_name or account.token[:12]}: quota probe failed: {exc}")
                return
            self.store.set_account_quota(
                account.token, quota_max, quota_left, quota_reset)
            return

    async def probe_quota(self) -> None:
        # 627 個帳號全量探測 = 每 300s 兩千多個 API call，URnetwork 會回 429。
        # 只探「已分配」+ 一小批輪流的 free 帳號；未探到的沿用上次數字。
        accounts = self.store.accounts()
        if not accounts:
            return
        assigned = [a for a in accounts if a.assigned_to]
        free = [a for a in accounts if not a.assigned_to]
        random.shuffle(free)
        batch = assigned + free[:PROBE_SAMPLE]
        semaphore = asyncio.Semaphore(PROBE_CONCURRENCY)

        async def one(account: Account) -> None:
            async with semaphore:
                await self.probe_account(account)

        await asyncio.gather(*(one(a) for a in batch))

    # -- allocation --------------------------------------------------------

    def allocate_tokens(self, uid: str, count: int) -> list[str]:
        """Hand out the healthiest unassigned accounts.

        An account whose quota is still unknown is handed out too: the
        figures only become known after the first probe, and refusing to
        allocate before then would leave the pool unusable.
        """
        free = [a for a in self.store.accounts()
                if a.state == "free" and (a.quota_left > 0 or a.quota_max == 0)]
        free.sort(key=lambda a: (-a.quota_left, a.network_name))
        chosen = free[:count]
        for account in chosen:
            self.store.set_account_state(account.token, "assigned", uid)
        return [a.token for a in chosen]

    def release_tokens(self, tokens: list[str]) -> None:
        for token in tokens:
            account = next((a for a in self.store.accounts() if a.token == token), None)
            if account is None:
                continue
            if account.quota_left <= 0:
                self.store.set_account_state(token, "dead")
            else:
                self.store.set_account_state(token, "free")

    # -- backends ----------------------------------------------------------

    def spawn_backend(self, inst: Instance, tokens: list[str],
                      internal_port: int) -> Backend:
        cmd = [self.args.urnproxy,
               "--addr", f"127.0.0.1:{internal_port}"]
        if inst.proxy:
            cmd += ["--provider-id", inst.proxy]
        elif inst.country:
            cmd += ["--country", inst.country]
        log(f"starting urnproxy for {inst.uid}: {' '.join(cmd[1:])}")
        os.makedirs(self.args.log_dir, exist_ok=True)
        logpath = os.path.join(self.args.log_dir, safe_name(inst.uid) + ".log")
        env = os.environ.copy()
        # shortcut: urnproxy logs in once per process, so one account per instance
        env["USER_AUTH"] = tokens[0] if tokens else ""
        env["PASSWORD"] = "urn-pool"
        logfile = open(logpath, "ab", buffering=0)
        proc = subprocess.Popen(cmd, stdout=logfile, stderr=subprocess.STDOUT,
                                start_new_session=True, env=env)
        return Backend(proc, internal_port, tokens)

    async def wait_ready(self, port: int, timeout: float = BACKEND_READY_TIMEOUT) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", port), timeout=1.0)
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                return True
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.2)
        return False

    async def retire_backend(self, inst: Instance, backend: Backend | None) -> None:
        """Wait for the old backend's connections to drain, then stop it."""
        if backend is None:
            return
        deadline = time.monotonic() + DRAIN_TIMEOUT
        while time.monotonic() < deadline:
            if backend.proc.poll() is not None:
                break
            if inst.front is not None and inst.front.relay_count(("127.0.0.1", backend.port)) == 0:
                break
            await asyncio.sleep(1.0)
        backend.stop()

    def sweep_orphans(self) -> int:
        """Kill leftover urnproxy children whose port is no live instance's.

        A failed swap or a crashed drain leaves the old adapter running; the
        pile then starves the URnetwork auth API and every later swap times out.
        Only our own children are touched, so other pools on the host are safe.
        """
        live = {inst.internal_port for inst in self.instances.values()}
        me = os.getpid()
        killed = 0
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                ppid = int(open(f"/proc/{entry}/stat", "rb").read()
                           .rsplit(b")", 1)[1].split()[1])
                if ppid != me:
                    continue
                argv = open(f"/proc/{entry}/cmdline", "rb").read().split(b"\0")
                if not argv or os.path.basename(argv[0]) != b"urnproxy":
                    continue
                port = int(argv[argv.index(b"--addr") + 1].rsplit(b":", 1)[1])
            except Exception:
                continue
            if port in live:
                continue
            with contextlib.suppress(ProcessLookupError):
                os.kill(int(entry), signal.SIGKILL)
                killed += 1
            with contextlib.suppress(ChildProcessError):
                os.waitpid(int(entry), 0)   # SIGKILL 不可攔，這裡不會卡；回收掉才不留 zombie
        if killed:
            log(f"  swept {killed} orphan urnproxy process(es)")
        return killed

    async def swap_backend(self, inst: Instance, tokens: list[str],
                           internal_port: int, retried: bool = False) -> bool:
        """Start a replacement and move the relay over to it.

        The old instance keeps the connections it already carries; the relay
        only sends new ones to the replacement.
        """
        old = inst.backend
        backend = self.spawn_backend(inst, tokens, internal_port)
        if not await self.wait_ready(internal_port):
            backend.stop()
            self.sweep_orphans()
            if not retried:
                # URnetwork 常以 429 / 掛住擋掉同一顆帳號的重新登入；
                # 換一顆帳號再試一次，否則這次 rotate 就是白白失敗。
                fresh = self.allocate_tokens(inst.uid, ACCOUNTS_PER_INSTANCE)
                if fresh and set(fresh) - set(tokens):
                    log(f"  {inst.uid}: spawn failed, retrying with a fresh account")
                    ok = await self.swap_backend(
                        inst, fresh,
                        self.store.next_port("internal_port", INTERNAL_PORT_BASE),
                        retried=True)
                    if ok:
                        self.release_tokens(tokens)
                    else:
                        for token in fresh:
                            self.store.set_account_state(token, "free")
                    return ok
            log(f"  {inst.uid}: replacement failed to start, keeping the old instance")
            return False
        inst.backend = backend
        if inst.front is not None:
            inst.front.point_at(("127.0.0.1", internal_port))
        inst.tokens = tokens
        inst.internal_port = internal_port
        inst.rotated_at = time.time()
        self.store.put_instance(inst)
        asyncio.create_task(self.retire_backend(inst, old))
        return True

    # -- instance lifecycle -------------------------------------------------

    async def create_instance(self, uid: str, country: str = "",
                              proxy: str = "") -> Instance:
        inst = Instance(uid=uid,
                        public_port=self.store.next_port("public_port", PUBLIC_PORT_BASE),
                        internal_port=self.store.next_port("internal_port", INTERNAL_PORT_BASE),
                        country=country, proxy=proxy, created_at=time.time())
        tokens = self.allocate_tokens(uid, ACCOUNTS_PER_INSTANCE)
        if len(tokens) < ACCOUNTS_PER_INSTANCE:
            log(f"  {uid}: only {len(tokens)} free account(s) available")
        inst.tokens = tokens
        self.store.put_instance(inst)

        front = Front(self.args.bind_host, inst.public_port)
        await front.start()
        inst.front = front

        if await self.swap_backend(inst, tokens, inst.internal_port):
            log(f"  {uid}: ready on {self.args.bind_host}:{inst.public_port} "
                f"({len(tokens)} accounts)")
        else:
            log(f"  {uid}: urnproxy did not come up on port {inst.internal_port}")
        self.instances[uid] = inst
        return inst

    async def delete_instance(self, uid: str) -> bool:
        inst = self.instances.get(uid) or self.store.instance(uid)
        if inst is None:
            return False
        if inst.front is not None:
            await inst.front.close()
        if inst.backend is not None:
            inst.backend.stop()
        self.release_tokens(inst.tokens)
        self.store.delete_instance(uid)
        self.instances.pop(uid, None)
        log(f"  {uid}: deleted, {len(inst.tokens)} account(s) released")
        return True

    async def rotate_upstream(self, uid: str, country: str = "",
                              proxy: str = "") -> bool:
        """Point a client at a different upstream without dropping it."""
        inst = self.instances.get(uid) or self.store.instance(uid)
        if inst is None:
            return False
        if proxy and country:
            raise ValueError("country and proxy are mutually exclusive")
        self.sweep_orphans()
        # Neither given keeps the current selection: the restart below still
        # re-picks a provider set, which is how /rotate gets a new exit IP.
        inst.country = country
        inst.proxy = proxy
        internal = self.store.next_port("internal_port", INTERNAL_PORT_BASE)
        ok = await self.swap_backend(inst, inst.tokens, internal)
        if ok:
            what = proxy or country
            log(f"  {uid}: now using {what} (public port unchanged)")
        return ok

    async def rotate_tokens(self, inst: Instance) -> bool:
        """Give a client a fresh set of accounts."""
        tokens = self.allocate_tokens(inst.uid, ACCOUNTS_PER_INSTANCE)
        if not tokens:
            log(f"  {inst.uid}: no free account left, keeping the current set")
            return False
        internal = self.store.next_port("internal_port", INTERNAL_PORT_BASE)
        old_tokens = list(inst.tokens)
        ok = await self.swap_backend(inst, tokens, internal)
        if ok:
            self.release_tokens(old_tokens)
        else:
            for token in tokens:
                self.store.set_account_state(token, "free")
        return ok

    # -- rotation policy ----------------------------------------------------

    async def check_rotations(self) -> None:
        if self._rotate_lock.locked():
            return
        async with self._rotate_lock:
            for inst in list(self.instances.values()):
                if not inst.tokens:
                    continue
                accounts = {a.token: a for a in self.store.accounts()}
                best = max((accounts[t].quota_left for t in inst.tokens
                             if t in accounts), default=0)
                limit = max((accounts[t].quota_max for t in inst.tokens
                             if t in accounts), default=0)
                if limit and best < limit * ROTATE_QUOTA_FRACTION:
                    log(f"  {inst.uid}: quota low ({best}/{limit}), rotating accounts")
                    await self.rotate_tokens(inst)
                    await asyncio.sleep(1.0)

    # -- supervision --------------------------------------------------------

    async def supervise(self) -> None:
        """Restart a backend that died, and adopt the ones that are alive."""
        for inst in list(self.instances.values()):
            if inst.backend is not None and not inst.backend.alive():
                log(f"  {inst.uid}: urnproxy exited, restarting")
                internal = self.store.next_port("internal_port", INTERNAL_PORT_BASE)
                await self.swap_backend(inst, inst.tokens, internal)

    # -- startup ------------------------------------------------------------

    def kill_our_backends(self) -> None:
        """Kill urnproxy processes we started earlier but no longer track."""
        marker = os.path.basename(self.args.urnproxy).encode()
        killed = 0
        for name in os.listdir("/proc"):
            if not name.isdigit():
                continue
            try:
                with open(f"/proc/{name}/cmdline", "rb") as fh:
                    argv = fh.read().split(b"\0")
            except OSError:
                continue
            # 只認 argv[0]，避免把命令列剛好含這個字樣的 shell/ssh 一起殺掉
            if not argv or os.path.basename(argv[0]) != marker:
                continue
            with contextlib.suppress(Exception):
                os.kill(int(name), signal.SIGKILL)   # SIGTERM 它不理，會留殘骸
                killed += 1
        if killed:
            log(f"stopped {killed} leftover urnproxy process(es)")

    async def start(self) -> None:
        self.refresh_accounts()
        # 不在啟動時探測額度：db 裡的數字是上一輪的，可用；啟動時先探 39 個帳號
        # 會把 URnetwork 的 login 額度燒掉，接著 15 個 backend 全部 429 起不來。
        self.kill_our_backends()
        for inst in self.store.instances():
            tokens = inst.tokens
            if not tokens:
                tokens = self.allocate_tokens(inst.uid, ACCOUNTS_PER_INSTANCE)
            front = Front(self.args.bind_host, inst.public_port)
            await front.start()
            inst.front = front
            # 先登記再換，sweep_orphans 才知道這個埠是活的
            self.instances[inst.uid] = inst
            if not await self.swap_backend(inst, tokens, inst.internal_port):
                log(f"  {inst.uid}: urnproxy did not come up")
            inst.tokens = tokens
            self.store.put_instance(inst)
            log(f"  {inst.uid}: restored on {self.args.bind_host}:{inst.public_port}")

    async def main_loop(self) -> None:
        while True:
            await asyncio.sleep(self.args.poll_interval)
            try:
                self.refresh_accounts()
                await self.probe_quota()
                await self.supervise()
                await self.check_rotations()
            except Exception as exc:  # noqa: BLE001
                log(f"poll cycle failed: {exc}")

    # -- HTTP ---------------------------------------------------------------

    async def handle(self, path: str, query: dict[str, list[str]]) -> tuple[int, str, str]:
        token = self.args.ui_token
        via_token = False
        if token:
            root = "/" + token
            if path.startswith(root + "/"):
                path = path[len(root):]
                via_token = True
        one = {k: v[0] for k, v in query.items() if v}
        if path == "/register":
            uid = one.get("uid", "").strip()
            if not uid:
                return 400, "text/plain", "uid is required\n"
            if uid in self.instances:
                inst = self.instances[uid]
                return 200, "application/json", json.dumps(self._instance_json(inst), indent=2)
            inst = await self.create_instance(uid,
                                              one.get("country") or DEFAULT_COUNTRY,
                                              one.get("proxy", ""))
            return 200, "application/json", json.dumps(self._instance_json(inst), indent=2)
        if path == "/delete":
            uid = one.get("uid", "").strip()
            if not uid:
                return 400, "text/plain", "uid is required\n"
            ok = await self.delete_instance(uid)
            return (200 if ok else 404), "application/json", json.dumps({"deleted": ok})
        if path == "/sub":
            uid = one.get("uid", "").strip()
            return 200, "text/yaml", self.subscription(uid or None)
        if path == "/locations":
            try:
                locations = await asyncio.to_thread(self.location_list)
            except Exception as exc:  # noqa: BLE001
                return 500, "text/plain", f"locations unavailable: {exc}\n"
            return 200, "application/json", json.dumps(locations, indent=2)
        if path == "/providers":
            country = one.get("country") or "us"
            limit = int(one.get("limit") or 600)
            try:
                providers = await asyncio.to_thread(self.provider_list, country, limit)
            except Exception as exc:  # noqa: BLE001
                return 500, "text/plain", f"providers unavailable: {exc}\n"
            return 200, "application/json", json.dumps(providers)
        if path == "/status":
            return 200, "application/json", json.dumps(self.status(), indent=2)
        if path == "/country":
            uid = one.get("uid", "").strip()
            if not uid:
                return 400, "text/plain", "uid is required\n"
            # a pinned provider carries its own location, so country defaults
            # only when no proxy id was given
            country = one.get("country") or ("" if one.get("proxy") else DEFAULT_COUNTRY)
            try:
                ok = await self.rotate_upstream(uid, country, one.get("proxy", ""))
            except ValueError as exc:
                return 400, "text/plain", f"{exc}\n"
            if not ok:
                return 404, "text/plain", "no such client\n"
            inst = self.instances[uid]
            return 200, "application/json", json.dumps(self._instance_json(inst), indent=2)
        if path == "/rotate":
            uid = one.get("uid", "").strip()
            if not uid:
                return 400, "text/plain", "uid is required\n"
            inst = self.instances.get(uid)
            if inst is None:
                return 404, "text/plain", "no such client\n"
            # 全池隨機：加權抽國家 → 隨機釘一顆 provider；撥不起來就重抽，
            # 三次都失敗才退回原本「同選擇重開」的行為。
            orig = (inst.country, inst.proxy)
            ok = False
            for _ in range(3):
                pid = await asyncio.to_thread(self.random_provider)
                if not pid:
                    continue
                if not await self.rotate_upstream(uid, "", pid):
                    continue
                # full-pool picks include contract-but-dead providers; verify
                # the new exit actually passes traffic before accepting it
                if await asyncio.to_thread(self.exit_alive, inst):
                    ok = True
                    break
            if not ok:
                ok = await self.rotate_upstream(uid, *orig)
            return (200 if ok else 500), "application/json", json.dumps(
                self._instance_json(inst), indent=2)
        return 404, "text/plain", "not found\n"

    def _instance_json(self, inst: Instance) -> dict:
        return {
            "uid": inst.uid,
            "port": inst.public_port,
            "proxy": f"socks5://{self.args.bind_host}:{inst.public_port}",
            "country": inst.country,
            "upstream": inst.proxy,
            "accounts": len(inst.tokens),
            "created_at": inst.created_at,
            "rotated_at": inst.rotated_at,
            "backend_alive": bool(inst.backend and inst.backend.alive()),
        }

    def subscription(self, uid: str | None) -> str:
        instances = ([self.instances[uid]] if uid and uid in self.instances
                     else list(self.instances.values()) if not uid else [])
        lines = ["proxies:"]
        for inst in instances:
            name = f"urn-{inst.uid}"
            lines += [
                f'  - name: "{name}"',
                "    type: socks5",
                f"    server: {self.args.bind_host}",
                f"    port: {inst.public_port}",
                "    udp: false",
            ]
        lines += ["proxy-groups:"]
        lines += ["  - name: urn", "    type: select", "    proxies:"]
        for inst in instances:
            lines.append(f'      - "urn-{inst.uid}"')
        lines += ["rules:", "  - MATCH,urn"]
        return "\n".join(lines) + "\n"

    def location_list(self, max_age: float = 600.0) -> list[dict]:
        """Provider locations straight from the URnetwork API."""
        now = time.time()
        if self._servers and now - self._servers[0] < max_age:
            return self._servers[1]
        account = next((a for a in self.store.accounts()), None)
        jwt = self._login(account.token) if account else ""
        req = urllib.request.Request(
            API_URL + "/network/provider-locations",
            headers={"Authorization": "Bearer " + jwt})
        resp = json.load(urllib.request.urlopen(req, timeout=30))
        out = [{"name": l["name"], "code": l.get("country_code", ""),
                "type": l["location_type"],
                "providers": l.get("provider_count", 0)}
               for l in resp.get("locations", [])
               if l.get("location_type") == "country" and l.get("provider_count")]
        out.sort(key=lambda l: l["name"])
        self._servers = (now, out)
        return out

    def provider_list(self, country: str = "us", limit: int = 600,
                      max_age: float = 600.0) -> list[dict]:
        """Provider ids for one country, paged through /network/find-providers2.

        Pinning one of these ids (``--provider-id``) beats the adapter's own
        pick, which always lands on the same top-ranked few providers.
        ``limit=0`` pages until the API runs out.  On a cached full list that
        is incremental: the cache is kept and only NEW providers are paged
        (shortcut: offline providers never leave the cache; /rotate's retry
        skips them when a pinned dial fails).
        """
        key = str(country).lower()
        now = time.time()
        cached = self._providers.get(key)
        if cached and now - cached[0] < max_age:
            return cached[1]
        accounts = list(self.store.accounts())
        account = random.choice(accounts) if accounts else None
        jwt = self._login(account.token) if account else ""
        req = urllib.request.Request(
            API_URL + "/network/provider-locations",
            headers={"Authorization": "Bearer " + jwt})
        locations = json.load(urllib.request.urlopen(req, timeout=30)).get("locations", [])
        match = next((l for l in locations
                      if key in ((l.get("country_code") or "").lower(),
                                 (l.get("name") or "").lower())), None)
        if match is None:
            return []
        if limit > 0 or not cached:
            out: list[dict] = []
            exclude: list[str] = []
        else:  # incremental refresh: page only providers missing from the cache
            out = list(cached[1])
            exclude = [p["id"] for p in out]
        backoffs = 0
        while limit <= 0 or len(out) < int(limit):
            body = json.dumps({"specs": [{"location_id": match["location_id"]}],
                               "count": 100,
                               "exclude_client_ids": exclude}).encode()
            req = urllib.request.Request(
                API_URL + "/network/find-providers2", data=body,
                headers={"Content-Type": "application/json",
                         "Authorization": "Bearer " + jwt})
            try:
                got = json.load(urllib.request.urlopen(req, timeout=60)).get("providers") or []
            except urllib.error.HTTPError as exc:
                # background full enumeration backs off on rate limiting;
                # the per-request quick path fails fast instead
                if limit <= 0 and exc.code == 429 and backoffs < 10:
                    backoffs += 1
                    log(f"  providers {key}: 429, backing off ({backoffs}/10)")
                    time.sleep(30)
                    continue
                raise
            backoffs = 0
            if not got:
                break
            for p in got:
                loc = p.get("location") or {}
                exclude.append(p["client_id"])
                out.append({"id": p["client_id"], "tier": int(p.get("tier") or 0),
                            "bps": int(p.get("estimated_bytes_per_second") or 0),
                            "city": loc.get("city", ""), "region": loc.get("region", "")})
            time.sleep(0.25)
        self._providers[key] = (now, out)
        log(f"  providers {key}: {len(out)} enumerated")
        return out

    def exit_alive(self, inst: Instance, timeout: float = 12.0) -> bool:
        """True if the instance's current exit passes traffic: SOCKS5 CONNECT
        to 1.1.1.1:443 through the public front port; a dead provider fails
        the dial (this sandboxes are the tail of the pool, lots of those)."""
        import socket
        try:
            with socket.create_connection(
                    (self.args.bind_host, inst.public_port), timeout=timeout) as s:
                s.settimeout(timeout)
                s.sendall(b"\x05\x01\x00")
                if s.recv(2) != b"\x05\x00":
                    return False
                s.sendall(b"\x05\x01\x00\x01" + socket.inet_aton("1.1.1.1")
                          + (443).to_bytes(2, "big"))
                resp = s.recv(4)
                return len(resp) >= 2 and resp[1] == 0
        except OSError:
            return False

    WARM_TTL = 900.0      # warmer refreshes a country at most this often (incremental)
    WARM_STALE = 14400.0  # rotate tolerates a warmed list up to this age

    def provider_warmer(self) -> None:
        """Background: full per-country provider lists (limit=0), biggest
        countries first.  /rotate reads these caches so a rotate never
        blocks on a 6-minute US enumeration."""
        while True:
            try:
                locations = self.location_list()
            except Exception as exc:  # noqa: BLE001
                log(f"warmer: locations unavailable: {exc}")
                time.sleep(60)
                continue
            now = time.time()
            todo = [l for l in locations
                    if (c := self._providers.get(l["name"].lower())) is None
                    or now - c[0] >= self.WARM_TTL]
            todo.sort(key=lambda l: -l["providers"])
            for l in todo:
                try:
                    self.provider_list(l["name"], limit=0, max_age=self.WARM_TTL)
                except Exception as exc:  # noqa: BLE001
                    log(f"warmer: {l['name']}: {exc}")
                time.sleep(1)
            time.sleep(300)

    def random_provider(self) -> str:
        """A random provider from the whole pool.

        Prefers urn-hub's open set (hub-open.json: seen in the latest global
        fetch AND not daemon-proven dead AND exit risk low); falls back to
        sampling a country weighted by registered provider count, then a
        random provider in that country (warmer cache, else quick top-600).
        "" = caller re-picks."""
        try:
            with open(HUB_OPEN, encoding="utf-8") as fh:
                doc = json.load(fh)
            # stale beyond one missed hub round: the fetcher died, use fallback
            if time.time() - doc["updated_at"] < 3900 and doc.get("providers"):
                return random.choice(doc["providers"])["id"]
        except (OSError, ValueError, KeyError):
            pass
        locations = self.location_list()
        total = sum(l["providers"] for l in locations)
        if not total:
            return ""
        pick = random.uniform(0, total)
        for l in locations:
            pick -= l["providers"]
            if pick <= 0:
                break
        providers = self.provider_list(l["name"], limit=600, max_age=self.WARM_STALE)
        if not providers:
            return ""
        return random.choice(providers)["id"]

    def status(self) -> dict:
        accounts = self.store.accounts()
        insts = []
        for inst in self.instances.values():
            item = self._instance_json(inst)
            item["relays"] = inst.front.total_relays() if inst.front is not None else 0
            insts.append(item)
        return {
            "host": self.args.bind_host,
            "instances": insts,
            "accounts": {
                "total": len(accounts),
                "free": sum(1 for a in accounts if a.state == "free"),
                "assigned": sum(1 for a in accounts if a.state == "assigned"),
                "dead": sum(1 for a in accounts if a.state == "dead"),
                "quota_left_bytes": sum(a.quota_left for a in accounts),
            },
        }


# ── HTTP plumbing ───────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    wrapper: Wrapper = None            # type: ignore[assignment]
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        log("http " + (fmt % args))

    def _respond(self, code: int, ctype: str, body: str,
                 headers: dict[str, str] | None = None) -> None:
        payload = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)

    def _dispatch(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        token = self.wrapper.args.ui_token
        if token and parsed.path == "/" + token:
            # A trailing slash keeps the page's relative URLs inside the token.
            self._respond(302, "text/plain", "", {"Location": "/" + token + "/"})
            return
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.wrapper.handle(parsed.path, query), self.wrapper.loop)
            code, ctype, body = future.result(timeout=120)
        except Exception as exc:  # noqa: BLE001
            log(f"request failed: {exc}")
            code, ctype, body = 500, "text/plain", f"{exc}\n"
        self._respond(code, ctype, body)

    do_GET = _dispatch
    do_POST = _dispatch
    do_DELETE = _dispatch


def detect_bind_host() -> str | None:
    """Find this host's Tailscale address.

    Binding the wrong address is worse than not binding at all -- a relay
    on 127.0.0.1 is unreachable from the tailnet -- so the lookup tries
    several ways and the caller refuses to start if none of them works.
    """
    for binary in ("tailscale", "/usr/bin/tailscale", "/usr/local/bin/tailscale"):
        try:
            out = subprocess.run([binary, "ip", "-4"], capture_output=True,
                                 text=True, timeout=10).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            continue
        if out:
            return out.splitlines()[0].strip()

    try:
        out = subprocess.run(["ip", "-4", "-o", "addr", "show"], capture_output=True,
                             text=True, timeout=10).stdout
        for line in out.splitlines():
            if "tailscale" not in line:
                continue
            parts = line.split()
            if len(parts) >= 4 and parts[2] == "inet":
                return parts[3].split("/")[0]
    except (OSError, subprocess.SubprocessError):
        pass

    # Tailscale hands out addresses in 100.64.0.0/10.
    import socket
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            octets = info[4][0].split(".")
            if len(octets) == 4 and octets[0] == "100" and 64 <= int(octets[1]) <= 127:
                return info[4][0]
    except OSError:
        pass
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="urn pool wrapper")
    ap.add_argument("--pool", default=DEFAULT_POOL,
                    help="account seedphrases, one per line")
    ap.add_argument("--auth-dir", default=DEFAULT_AUTH_DIR)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    ap.add_argument("--urnproxy", default=DEFAULT_URNPROXY)
    ap.add_argument("--bind-host", default=None,
                    help="address the API and the relays listen on "
                         "(default: this host's Tailscale address)")
    ap.add_argument("--bind-wait", type=float, default=90.0,
                    help="seconds to wait for the Tailscale address to appear")
    ap.add_argument("--api-port", type=int, default=8090)
    ap.add_argument("--poll-interval", type=float, default=300.0,
                    help="seconds between balance polls")
    ap.add_argument("--ui-token", default="",
                    help="alias every API path under /<token>/ "
                         "(empty: bare paths only)")
    args = ap.parse_args(argv)
    if args.ui_token and (not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.ui_token)
                          or args.ui_token in RESERVED_PATHS):
        print("--ui-token must be 1-64 chars of [A-Za-z0-9_-] and not an API path name",
              file=sys.stderr)
        return 2
    if args.bind_host is None:
        # After a boot the wrapper can start before tailscaled has come up.
        # Waiting a little is better than binding the wrong address, and
        # failing loudly is better still: a relay on 127.0.0.1 cannot be
        # reached from the tailnet, so it looks alive but serves nobody.
        deadline = time.time() + args.bind_wait
        while True:
            args.bind_host = detect_bind_host()
            if args.bind_host:
                break
            if time.time() >= deadline:
                print("could not determine the Tailscale address; "
                      "pass --bind-host explicitly", file=sys.stderr)
                return 2
            log("waiting for the Tailscale address...")
            time.sleep(5)

    os.makedirs(args.log_dir, exist_ok=True)
    os.chmod(args.log_dir, 0o700)
    for path in (args.db,):
        if os.path.exists(path):
            os.chmod(path, 0o600)

    wrapper = Wrapper(args)
    loop = asyncio.new_event_loop()
    wrapper.loop = loop
    asyncio.set_event_loop(loop)

    Handler.wrapper = wrapper
    server = ThreadingHTTPServer((args.bind_host, args.api_port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Thread(target=wrapper.provider_warmer, daemon=True,
                     name="provider-warmer").start()
    log(f"api on http://{args.bind_host}:{args.api_port}")

    async def run() -> None:
        await wrapper.start()
        await wrapper.main_loop()

    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
