"""Tor proxy manager with automatic circuit renewal for IP rotation."""

import logging
import time

import aiohttp

logger = logging.getLogger(__name__)


class TorManager:
    """Manages Tor SOCKS5 proxy with automatic circuit renewal."""

    MIN_RENEWAL_INTERVAL = 15.0

    def __init__(self, socks_port: int = 9150, control_port: int = 9051,
                 password: str = "", renew_every: int = 20,
                 renew_on_ratelimit: bool = True):
        self._socks_port = socks_port
        self._control_port = control_port
        self._password = password
        self._renew_every = renew_every
        self._renew_on_ratelimit = renew_on_ratelimit
        self._socks_proxy = f"socks5://127.0.0.1:{socks_port}"
        self._queries_since_renewal = 0
        self._circuits_renewed = 0
        self._last_renewal_time = 0.0
        self._is_available = False

    async def check_availability(self) -> bool:
        """Test if Tor proxy is reachable."""
        try:
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(self._socks_proxy)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    "https://check.torproject.org/api/ip",
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    data = await resp.json()
                    self._is_available = data.get("IsTor", False)
                    if self._is_available:
                        logger.info(f"Tor connected. Exit IP: {data.get('IP', 'unknown')}")
                    return self._is_available
        except Exception as e:
            logger.warning(f"Tor not available: {e}")
            self._is_available = False
            return False

    @property
    def is_available(self) -> bool:
        return self._is_available

    def get_proxy_url(self) -> str:
        return self._socks_proxy

    def get_ddgs_proxy(self) -> str:
        """Return proxy string for duckduckgo-search library."""
        if self._socks_port == 9150:
            return "tb"
        return self._socks_proxy

    def renew_circuit(self) -> bool:
        """Request a new Tor circuit via the control port."""
        now = time.time()
        if now - self._last_renewal_time < self.MIN_RENEWAL_INTERVAL:
            return False

        try:
            from stem import Signal
            from stem.control import Controller

            with Controller.from_port(port=self._control_port) as controller:
                if self._password:
                    controller.authenticate(password=self._password)
                else:
                    controller.authenticate()
                controller.signal(Signal.NEWNYM)

            self._circuits_renewed += 1
            self._last_renewal_time = time.time()
            self._queries_since_renewal = 0
            logger.info(
                f"Tor circuit renewed (#{self._circuits_renewed}). "
                "Waiting 15s for new circuit..."
            )
            time.sleep(15)
            return True
        except Exception as e:
            logger.warning(f"Tor circuit renewal failed: {e}")
            return False

    def should_renew(self) -> bool:
        return self._queries_since_renewal >= self._renew_every

    def on_search_completed(self):
        self._queries_since_renewal += 1

    def on_ratelimit(self) -> bool:
        if self._renew_on_ratelimit and self._is_available:
            return self.renew_circuit()
        return False
