import asyncio
import gzip
import json
from loguru import logger
from asyncio import sleep
import time
from enum import Enum
from random import random
from typing import Optional, List, Callable

from socket import gaierror
from .exceptions import FtxWebsocketUnableToConnect
from websockets.exceptions import ConnectionClosedError

import websockets as ws
from ftx import AsyncClient
from ftx import ws_signature
from .threaded_stream import ThreadedApiManager

KEEPALIVE_TIMEOUT = 5 * 60  # 5 minutes


class WSListenerState(Enum):
    INITIALISING = "Initialising"
    STREAMING = "Streaming"
    RECONNECTING = "Reconnecting"
    EXITING = "Exiting"


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10
    NO_MESSAGE_RECONNECT_TIMEOUT = 60
    MAX_QUEUE_SIZE = 10000

    def __init__(
        self,
        loop,
        url: str,
        subscription: List,
        name: Optional[str] = None,
        is_binary: bool = False,
        exit_coro=None,
    ):
        self._loop = loop or asyncio.get_event_loop()
        self._name = name
        self._url = url
        self._exit_coro = exit_coro
        self._reconnects = 0
        self._is_binary = is_binary
        self._conn = None
        self.ws: Optional[ws.WebSocketClientProtocol] = None
        self.ws_state = WSListenerState.INITIALISING
        self._queue = asyncio.Queue(loop=self._loop)
        self._handle_read_loop = None
        self.subscription = subscription

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._exit_coro:
            await self._exit_coro(self._name)
        self.ws_state = WSListenerState.EXITING
        if self.ws:
            self.ws.fail_connection()
        if self._conn and hasattr(self._conn, "protocol"):
            await self._conn.__aexit__(exc_type, exc_val, exc_tb)
        self.ws = None
        if not self._handle_read_loop:
            logger.error("CANCEL read_loop")
            await self._kill_read_loop()

    async def connect(self):
        await self._before_connect()
        assert self._name
        self.ws_state = WSListenerState.STREAMING
        self._conn = ws.connect(self._url, close_timeout=0.1)
        try:
            self.ws = await self._conn.__aenter__()
        except:  # noqa
            await self._reconnect()
            return
        self._reconnects = 0
        await self._after_connect()
        if not self._handle_read_loop:
            self._handle_read_loop = self._loop.call_soon_threadsafe(asyncio.create_task, self._read_loop())

    async def _kill_read_loop(self):
        self.ws_state = WSListenerState.EXITING
        while self._handle_read_loop:
            await sleep(0.1)

    async def send_msg(self, msg):
        wait_count = 0
        while not self.ws:
            time.sleep(0.1)
            wait_count = wait_count + 1
            if wait_count > 20:
                raise Exception("send_msg() failed, cannot resend subscription")

        await self.ws.send(json.dumps(msg))

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        pass

    def _handle_message(self, evt):
        if self._is_binary:
            try:
                evt = gzip.decompress(evt)
            except (ValueError, OSError):
                return None
        try:
            return json.loads(evt)
        except ValueError:
            logger.debug(f"error parsing evt json:{evt}")
            return None

    async def _read_loop(self):
        try:
            while True:
                try:
                    if self.ws_state == WSListenerState.RECONNECTING:
                        await self._run_reconnect()
                    if not self.ws or self.ws_state != WSListenerState.STREAMING:
                        await self._wait_for_reconnect()
                        break
                    elif self.ws_state == WSListenerState.EXITING:
                        break
                    elif self.ws.state == ws.protocol.State.CLOSING:
                        await asyncio.sleep(0.1)
                        continue
                    elif self.ws.state == ws.protocol.State.CLOSED:
                        await self._reconnect()
                    elif self.ws_state == WSListenerState.STREAMING:
                        res = await asyncio.wait_for(self.ws.recv(), timeout=self.TIMEOUT)
                        res = self._handle_message(res)
                        if res:
                            if self._queue.qsize() < self.MAX_QUEUE_SIZE:
                                await self._queue.put(res)
                            else:
                                logger.debug(f"Queue overflow {self.MAX_QUEUE_SIZE}. Message not filled")
                                await self._queue.put({"e": "error", "m": "Queue overflow. Message not filled"})
                                raise FtxWebsocketUnableToConnect
                except asyncio.TimeoutError:
                    logger.debug(f"no message in {self.TIMEOUT} seconds")
                except asyncio.CancelledError as e:
                    logger.debug(f"cancelled error {e}")
                    break
                except asyncio.IncompleteReadError as e:
                    logger.debug(f"incomplete read error ({e})")
                except ConnectionClosedError as e:
                    logger.debug(f"connection close error ({e})")
                except gaierror as e:
                    logger.debug(f"DNS Error ({e})")
                except FtxWebsocketUnableToConnect as e:
                    logger.debug(f"FtxWebsocketUnableToConnect ({e})")
                    break
                except Exception as e:
                    logger.debug(f"Unknown exception ({e})")
                    continue
        finally:
            self._handle_read_loop = None
            self._reconnects = 0

    async def _after_reconnect(self):
        for msg in self.subscription:
            await self.send_msg(msg)

    async def _run_reconnect(self):
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            logger.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait} seconds to start."
            )
            await asyncio.sleep(reconnect_wait)
            await self.connect()
            await self._after_reconnect()
        else:
            logger.error(f"Max reconnections {self.MAX_RECONNECTS} reached:")
            # Signal the error
            await self._queue.put({"e": "error", "m": "Max reconnect retries reached"})
            raise FtxWebsocketUnableToConnect

    async def recv(self):
        res = None
        while not res:
            try:
                res = await asyncio.wait_for(self._queue.get(), timeout=self.TIMEOUT)
            except asyncio.TimeoutError:
                logger.debug(f"no message in {self.TIMEOUT} seconds")
        return res

    async def _wait_for_reconnect(self):
        while self.ws_state != WSListenerState.STREAMING and self.ws_state != WSListenerState.EXITING:
            await sleep(0.1)

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2 ** attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def before_reconnect(self):
        if self.ws:
            await self._conn.__aexit__(None, None, None)
            self.ws = None
        self._reconnects += 1

    def _no_message_received_reconnect(self):
        logger.debug("No message received, reconnecting")
        self.ws_state = WSListenerState.RECONNECTING

    async def _reconnect(self):
        self.ws_state = WSListenerState.RECONNECTING


class FtxSocketManager:
    WS_URL = "wss://ftx.com/ws/"

    def __init__(
        self,
        client: AsyncClient,
        loop=None,
    ):
        self._conns = {}
        self._loop = loop or asyncio.get_event_loop()
        self._client = client
        self.subscription = []

    def _init_stream_url(self):
        self.ws_url = self.STREAM_URL
        self.private_ws_url = self.PSTREAM_URL

    def _get_socket(self, socket_name: str, is_binary: bool = False) -> str:
        if socket_name not in self._conns:
            self._conns[socket_name] = ReconnectingWebsocket(
                loop=self._loop,
                name=socket_name,
                url=self.WS_URL,
                exit_coro=self._exit_socket,
                is_binary=is_binary,
                subscription=self.subscription,
            )

        return self._conns[socket_name]

    async def subscribe(self, socket_name: str, **params):
        try:
            await self._conns[socket_name].send_msg(params)
            if params not in self.subscription:
                self.subscription.append(params)
        except KeyError:
            logger.warning(f"Connection name: <{socket_name}> not create or start!")

    async def _exit_socket(self, name: str):
        await self._stop_socket(name)

    def get_socket(self, socket_name):
        return self._get_socket(socket_name)

    async def _stop_socket(self, conn_key):
        if conn_key not in self._conns:
            return

        del self._conns[conn_key]


class ThreadedWebsocketManager(ThreadedApiManager):
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        subaccount: str = None,
    ):
        super().__init__(api_key, api_secret)
        self._fsm: Optional[FtxSocketManager] = None
        self.api = api_key
        self.secret = api_secret
        self.subaccount = subaccount

    async def _before_socket_listener_start(self):
        assert self._client
        self._fsm = FtxSocketManager(client=self._client, loop=self._loop)

    def _start_socket(
        self,
        callback: Callable,
        socket_name: str,
    ) -> str:
        while not self._fsm:
            time.sleep(0.1)

        socket = getattr(self._fsm, "get_socket")(socket_name)
        name = socket._name
        self._socket_running[name] = True
        self._loop.call_soon_threadsafe(
            asyncio.create_task,
            self.start_listener(socket, socket._name, callback, self.ping),
        )

        return socket

    def start_socket(
        self,
        callback: Callable,
        socket_name: str,
    ) -> str:
        socket = self._start_socket(
            callback=callback,
            socket_name=socket_name,
        )
        self.ping(socket_name)
        return socket

    def subscribe(self, socket_name: str, **params):
        while not self._fsm:
            time.sleep(0.1)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            loop.create_task(self._fsm.subscribe(socket_name, **params))
        else:
            asyncio.run(self._fsm.subscribe(socket_name, **params))

    def login(self, socket_name: str):
        if not socket_name:
            return
        ts = int(time.time() * 1000)
        sign = ws_signature(ts, self.secret)
        args = {}
        args["key"] = self.api
        args["sign"] = sign
        args["time"] = ts
        if self.subaccount != None:
            args["subaccount"] = self.subaccount

        self.subscribe(socket_name=socket_name, args=args, op="login")

    def ping(self, name):
        self.subscribe(name, op="ping")
