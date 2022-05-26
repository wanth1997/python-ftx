import time
import json
import aiohttp
import asyncio
import requests
from loguru import logger
from ftx import signature
from urllib.parse import urlencode
from typing import Dict, Optional, List, Tuple
from ftx import FtxAPIException, FtxValueError
from concurrent.futures._base import TimeoutError


class BaseClient:
    API_URL = "https://ftx.com/api"

    def __init__(self, api: Optional[str] = None, secret: Optional[str] = None):
        self.API_KEY, self.API_SECRET = api, secret
        self.session = self._init_session()
        self.header = {}
        self.TIMEOUT = 45

    def _get_header(self) -> Dict:
        header = {
            "FTX-KEY": "",
            "FTX-SIGN": "",
            "FTX-TS": "",
        }
        if self.API_KEY:
            assert self.API_KEY
            header["FTX-KEY"] = self.API_KEY
        return header

    def _init_session(self):
        raise NotImplementedError

    def _init_url(self):
        self.api_url = self.API_URL

    def _handle_response(self, response: requests.Response):
        code = response.status_code
        if code == 200:
            return response.json()
        else:
            try:
                resp_json = response.json()
                raise FtxAPIException(resp_json, code)
            except ValueError:
                raise FtxValueError(response)


class Client(BaseClient):
    def __init__(self, api: Optional[str], secret: Optional[str], subaccount=None):
        super().__init__(api=api, secret=secret)
        self.subaccount = subaccount

    def _init_session(self) -> requests.Session:
        self.header = self._get_header()
        session = requests.session()
        session.headers.update(self.header)
        return session

    def _get(self, path: str, params=None):
        return self._request("get", path, params)

    def _post(self, path: str, params=None) -> Dict:
        return self._request("post", path, params)

    def _put(self, path: str, params=None) -> Dict:
        return self._request("put", path, params)

    def _delete(self, path: str, params=None) -> Dict:
        return self._request("delete", path, params)

    def _request(self, method, path: str, params: Dict):
        try:
            ts = str(int(time.time() * 1000))
            uri = f"{self.API_URL}{path}"
            sig = signature(ts, method, path, self.API_SECRET, params)
            self.header["FTX-KEY"] = self.API_KEY
            self.header["FTX-SIGN"] = sig
            self.header["FTX-TS"] = ts
            if self.subaccount is not None:
                self.header["FTX-SUBACCOUNT"] = self.subaccount
            self.session.headers.update(self.header)
            if method == "get":
                self.response = getattr(self.session, method)(uri, params=params)
            else:
                self.response = getattr(self.session, method)(uri, json=params)
            return self._handle_response(self.response)
        except Exception as e:
            logger.error(f"[ERROR] Request failed!")
            logger.error(self.response.text)

    def get_markets(self) -> Dict:
        return self._get("/markets")

    def get_funding_rate(self, **kwargs) -> Dict:
        return self._get("/funding_rates", params=kwargs)

    def get_klines(self, market: str, resolution: int, start_time=None, end_time=None) -> Dict:
        params = {}
        params["resolution"] = resolution
        if start_time is not None:
            params["start_time"] = start_time
        if end_time is not None:
            params["end_time"] = end_time

        return self._get(f"/markets/{market}/candles", params=params)

    def get_account_info(self) -> Dict:
        return self._get("/account")

    def get_all_balances(self) -> Dict:
        return self._get("/wallet/all_balances")

    def get_balances(self) -> Dict:
        return self._get("/wallet/balances")

    def get_positions(self, **kwargs) -> Dict:
        return self._get("/positions", params=kwargs)

    def send_order(self, **kwargs) -> Dict:
        try:
            return self._post("/orders", params=kwargs)
        except:
            time.sleep(0.2)
            return self._post("/orders", params=kwargs)

    def set_leverage(self, **kwargs):
        return self._post("/account/leverage", kwargs)

    def request_withdrawal(self, **kwargs):
        return self._post("/wallet/withdrawals", kwargs)

    def cancel_order(self, order_id):
        return self._delete(f"/orders/{order_id}")


class AsyncClient(BaseClient):
    def __init__(
        self,
        api: Optional[str],
        secret: Optional[str],
        subaccount=None,
        loop=None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.subaccount = subaccount
        super().__init__(
            api=api,
            secret=secret,
        )

    @classmethod
    async def create(
        cls,
        api: Optional[str],
        secret: Optional[str],
        loop=None,
    ):
        self = cls(api, secret, loop)
        return self

    def _init_session(self) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession(loop=self.loop, headers=self._get_header())
        return session

    async def close_connection(self):
        if self.session:
            assert self.session
            await self.session.close()

    async def _request(self, method, uri: str, path: str, params: Dict):
        try:
            ts = str(int(time.time() * 1000))
            uri = f"{self.API_URL}{path}"

            sig = signature(ts, method, path, self.API_SECRET)
            self.header["FTX-KEY"] = self.API_KEY
            self.header["FTX-SIGN"] = sig
            self.header["FTX-TS"] = ts
            if self.subaccount is not None:
                self.header["FTX-SUBACCOUNT"] = self.subaccount
            self.session.headers.update(self.header)

            async with getattr(self.session, method)(uri, params=params) as response:
                self.response = response
                return await self._handle_response(response)

        except Exception as e:
            logger.error(f"[ERROR] Request failed!")
            logger.error(e)

    async def _handle_response(self, response: requests.Response):
        code = response.status_code
        if code == 200:
            return response.json()
        else:
            try:
                resp_json = response.json()
                raise FtxAPIException(resp_json, code)
            except ValueError:
                raise FtxValueError(response)

    async def _get(self, path: str, params=None):
        return await self._request("get", path, params)

    async def _post(self, path: str, params=None) -> Dict:
        return await self._request("post", path, params)

    async def _put(self, path: str, params=None) -> Dict:
        return await self._request("put", path, params)

    async def _delete(self, path: str, params=None) -> Dict:
        return await self._request("delete", path, params)

    async def get_available_symbol(self):
        return await self._get("public/info")
