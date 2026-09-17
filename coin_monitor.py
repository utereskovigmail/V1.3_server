import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import random
import time
from typing import Optional
from curl_cffi.requests import AsyncSession


@dataclass
class MonitorResult:
    """Результат роботи монітора для передачі в модуль купівлі."""
    success: bool
    successful_session: Optional[AsyncSession] = None
    target_url: str = ""
    detected_at: Optional[datetime] = None
    proxy_used: Optional[str] = None


class CoinDropMonitor:
    def __init__(
            self,
            target_url: str,
            drop_time: datetime,
            proxies: list[str],
            base_url: str = "https://coins.bank.gov.ua",
            interval_ms: int = 500,
            max_users: int = 100,
            buffer_trigger_ms: int = 80 * 1000,
            captcha_buffer_ms: int = 180 * 1000,
            proxy_lifespan_ms: int = 10 * 60 * 1000,
    ):
        self.target_url = target_url
        self.drop_time = drop_time
        self.raw_proxies = proxies
        self.base_url = base_url
        self.interval_ms = interval_ms
        self.max_users = max_users

        self.buffer_trigger_ms = buffer_trigger_ms
        self.captcha_buffer_ms = captcha_buffer_ms
        self.proxy_lifespan_ms = proxy_lifespan_ms

        self._stop_event = asyncio.Event()
        self._successful_session: Optional[AsyncSession] = None
        self._successful_proxy: Optional[str] = None
        self._sessions: list[AsyncSession] = []
        self._proxy_map: dict[AsyncSession, str] = {}

    async def _test_single_proxy(self, proxy: str, semaphore: asyncio.Semaphore, timeout: float = 15.0) -> dict:
        async with semaphore:
            await asyncio.sleep(random.uniform(0.05, 0.3))
            start_t = time.perf_counter()
            try:
                async with AsyncSession(
                        proxies={"http": proxy, "https": proxy},
                        impersonate="chrome142",
                        timeout=timeout,
                        verify=True,
                ) as session:
                    response = await session.get(
                        self.target_url,
                        allow_redirects=False,
                        headers={
                            "referer": f"{self.base_url}/",
                            "sec-fetch-dest": "document",
                            "sec-fetch-mode": "navigate",
                            "sec-fetch-site": "same-origin",
                        },
                    )

                    latency = (time.perf_counter() - start_t) * 1000
                    body = response.text or ""

                    if response.status_code in (403, 429) or any(
                            x in body for x in
                            ["cf-mitigated", "Just a moment", "Checking your browser", "Attention Required"]
                    ):
                        return {"proxy": proxy, "status": "FAIL"}

                    if response.status_code >= 400 or len(body) < 3000:
                        return {"proxy": proxy, "status": "FAIL"}

                    return {"proxy": proxy, "status": "OK", "latency": latency}

            except Exception:
                return {"proxy": proxy, "status": "FAIL"}

    async def filter_best_proxies(self, max_concurrent: int = 15) -> list[str]:
        """Відсіює неробочі та заблоковані проксі."""
        print(f"[PROXY-CHECKER] Testing {len(self.raw_proxies)} proxies...")
        semaphore = asyncio.Semaphore(max_concurrent)
        results = await asyncio.gather(*(self._test_single_proxy(p, semaphore) for p in self.raw_proxies))

        valid = [r for r in results if r["status"] == "OK"]
        valid.sort(key=lambda x: x["latency"])

        print(f"🟢 Passed: {len(valid)}/{len(self.raw_proxies)}")
        return [item["proxy"] for item in valid]

    async def _execute_precise_countdown(self, ms_to_wait: float):
        target_time = (asyncio.get_event_loop().time() * 1000) + ms_to_wait
        while True:
            now_ms = asyncio.get_event_loop().time() * 1000
            remaining = target_time - now_ms
            if remaining <= 0:
                break
            if remaining > 1000:
                seconds_left = int(remaining / 1000)
                print(f"⏳ [COUNTDOWN] T-Minus {seconds_left}s до запуску моніторів...")
                await asyncio.sleep(1.0)
            else:
                await asyncio.sleep(remaining / 1000)
                break

    async def _make_request(self, session: AsyncSession, method: str, url: str, **kwargs):
        kwargs.setdefault("timeout", 35.0)
        method = method.upper()
        if method == "GET":
            return await session.get(url, **kwargs)
        elif method == "POST":
            return await session.post(url, **kwargs)
        return await session.request(method, url, **kwargs)

    async def _worker_request_task(self, session: AsyncSession, proxy_label: str, loop_num: int) -> bool:
        #change
        # self.target_url = 'https://coins.bank.gov.ua/-igri-hhhii-olimpiadi-u-phutljari/p-999.html'
        loop_start = time.perf_counter()
        log_prefix = f"[{proxy_label}] [LOOP #{loop_num}]"

        try:
            # change
            # if   self.drop_time + timedelta(minutes=1) <= datetime.now():
            #     self.target_url = 'https://coins.bank.gov.ua/-hersones-tavrijskij-u-phutljari/p-1016.html'
            response = await self._make_request(
                session=session,
                method="GET",
                url=self.target_url,
                allow_redirects=False,
                headers={
                    "referer": f"{self.base_url}/",
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "same-origin",
                },
            )
            #change
            # await asyncio.sleep(10)

            duration = int((time.perf_counter() - loop_start) * 1000)
            body_text = response.text if (response and hasattr(response, "text")) else ""
            body_lower = body_text.lower()

            if response and (response.status_code == 429 or "cf-mitigated" in body_lower):
                print(f"⚠️ {log_prefix} Rate limited or challenged ({duration}ms).")
                return False

            is_live = (
                    "btn-primary buy login" in body_lower or
                    "авторизуватися" in body_lower or
                    "купити" in body_lower
            )

            print(
                f"{log_prefix} [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Completed in {duration}ms. Live: {is_live}")

            if is_live:
                self._successful_session = session
                self._successful_proxy = self._proxy_map.get(session)
                self._stop_event.set()
                return True

        except Exception as err:
            duration = int((time.perf_counter() - loop_start) * 1000)
            print(f"❌ {log_prefix} Failed after {duration}ms: {err}")

        return False

    async def _run_pipeline(self, max_concurrent_tasks: int = 200):
        pool_size = len(self._sessions)
        print(f"🚀 Round-Robin Pipeline Monitor ініціалізовано на {pool_size} сесій.")

        loop_count = 0
        active_tasks = set()

        try:
            while not self._stop_event.is_set():
                if len(active_tasks) >= max_concurrent_tasks:
                    await asyncio.sleep(0.05)
                    continue

                loop_count += 1
                current_session = self._sessions[(loop_count - 1) % pool_size]
                proxy_label = f"Proxy #{(loop_count - 1) % pool_size + 1}"

                task = asyncio.create_task(
                    self._worker_request_task(
                        session=current_session,
                        proxy_label=proxy_label,
                        loop_num=loop_count,
                    )
                )
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

                jitter = random.uniform(-0.075, 0.075)
                sleep_duration = max(0.05, (self.interval_ms / 1000.0) + jitter)

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_duration)
                except asyncio.TimeoutError:
                    pass

        finally:
            for task in active_tasks:
                if not task.done():
                    task.cancel()
            print(f"🎯 State detected! Cancelled {len(active_tasks)} pending tasks.")

    async def start(self) -> MonitorResult:
        """Головний метод запуску. Чекає дропу, моніторить і повертає результат."""
        drop_ms = self.drop_time.timestamp() * 1000
        ms_until_release = drop_ms - (datetime.now().timestamp() * 1000)

        # 1. Сон до ліміту життя проксі
        if ms_until_release > self.proxy_lifespan_ms:
            excess_sleep = ms_until_release - self.proxy_lifespan_ms
            print(f"[⏰ TIME-LOCK] Сон на {excess_sleep / 1000 / 60:.1f} хв до валідації проксі.")
            await asyncio.sleep(excess_sleep / 1000)

        # 2. Тест проксі
        best_proxies = await self.filter_best_proxies()
        if not best_proxies:
            raise RuntimeError("Неможливо розпочати: немає робочих проксі!")

        best_proxies = best_proxies[: self.max_users]

        # 3. Буферний сон (CAPTCHA)
        ms_until_release = drop_ms - (datetime.now().timestamp() * 1000)
        if ms_until_release > self.captcha_buffer_ms:
            excess_sleep = ms_until_release - self.captcha_buffer_ms
            print(f"[⏰ TIME-LOCK] Сон на {excess_sleep / 1000 / 60:.1f} хв.")
            await asyncio.sleep(excess_sleep / 1000)

        # 4. Ініціалізація пулу сесій
        self._sessions = []
        self._proxy_map = {}
        for p in best_proxies:
            s = AsyncSession(proxies={"http": p, "https": p}, impersonate="chrome142", verify=True, timeout=35.0)
            self._sessions.append(s)
            self._proxy_map[s] = p

        try:
            # 5. Сон до тригеру монітора
            current_ms = drop_ms - (datetime.now().timestamp() * 1000)
            if current_ms > self.buffer_trigger_ms:
                print(f"[⏰ TIME-LOCK] Сон на {(current_ms - self.buffer_trigger_ms) / 1000 / 60:.1f} хв.")
                await asyncio.sleep((current_ms - self.buffer_trigger_ms) / 1000)

            # 6. Точний таймер за 10 сек
            target_sleep = (drop_ms - 10000) - (datetime.now().timestamp() * 1000)
            if target_sleep > 0:
                await self._execute_precise_countdown(target_sleep)

            # 7. Запуск циклу моніторингу
            await self._run_pipeline()

            # 8. Закриваємо всі сесії КРІМ успішної
            for s in self._sessions:
                if s != self._successful_session:
                    await s.close()

            return MonitorResult(
                success=True,
                successful_session=self._successful_session,
                target_url=self.target_url,
                detected_at=datetime.now(),
                proxy_used=self._successful_proxy,
            )

        except Exception as e:
            for s in self._sessions:
                await s.close()
            raise e