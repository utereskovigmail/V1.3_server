import asyncio
from datetime import datetime, timedelta
import random
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
import aiohttp
import time
import json
from pathlib import Path
from coin_monitor import CoinDropMonitor

#change
#добав проксі
#add accounts
CONFIG = {
    "SITEKEY": "0x4AAAAAAA6b9cm3XGGgkDP-",
    "BASE_URL": "https://coins.bank.gov.ua",
    "NUMBER_OF_CAPTCHAS": 1,
    "CoinIds": [838],
    "FirstCoinUrl": "https://coins.bank.gov.ua/pam-jatna-banknota-nominalom-20-grn-do-160-richchja-vid-dnja-narodzhennja-i-phranka/p-838.html",
    "MonitorIntervalMS": 1000,
    "CAPSOLVER_KEY": "CAP-BC1653549B7FEC08E255E96BB854831EBD9B38ABDD629F0C0FB0C5A1B5BF016E"
}
reload_startTime = datetime.now().replace(hour=20, minute=37, second=0, microsecond=0)
# log_filename = f'logs/{datetime.now().strftime("%Y%m%d-%H%M%S")}.log'
CAPTCHA_BUFFER_MS = 180 * 1000


def load_users_from_json(file_path: str | Path) -> list:
  BASE_DIR = Path(__file__).resolve().parent
  path = BASE_DIR / "other"/"run" / "users.json"
  if not path.exists():
    print(f'Помилка: файл {file_path} не знайдено.')
    return []


  try:
    with open(path, 'r', encoding='utf-8') as file:
      data = json.load(file)

      if isinstance(data, list):
        return data
      else:
        print('Попередження: вміст файлу не є списком (list).')
        return []

  except json.JSONDecodeError:
    print(f'Помилка: файл {file_path} містить некоректний JSON.')
    return []
  except Exception as e:
    print(f'Непередбачувана помилка: {e}')
    return []

async def keep_alive_and_verify(
        session,
        thread_log: list,
        log_prefix: str,
        user: dict,
        session_state: dict,
        url: str,
        session_lock: asyncio.Lock
):
    """Надсилає серцебиття (heartbeat) через make_request_with_retry, щоб тримати сесію живою перед дропом."""
    if session_lock.locked():
        return

    try:
        log_thread_event(thread_log, f"💓 [KEEP-ALIVE] Sending heartbeat ping to keep session fresh...", log_prefix)

        # Використовуємо твою стандартну функцію для запитів з ретраями
        response = await make_request(
            session=session,
            method="GET",
            url=url,
            log_prefix=log_prefix,
            thread_log=thread_log,
            headers={
                'referer': f"{CONFIG['BASE_URL']}/",
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
            }
        )
        # await asyncio.sleep(10)

        body_text = response.text if (response and hasattr(response, 'text')) else ""

        error_markers = ("429 помилка", "озволено запитів за", "озволено запитів за 1 секунд")
        if any(marker in body_text for marker in error_markers):
            log_thread_event(thread_log, "🚫 Rate limited detected. Skipping re-login, waiting for next cycle...",
                             log_prefix)
            await asyncio.sleep(1)
            return

        success_markers = ("account_edit.php", "account_password.php", "my_coins.php", "logoff.php")
        if any(marker in body_text for marker in success_markers):
            log_thread_event(thread_log, "✅ Session is active.", log_prefix)
            return

        async with session_lock:
            log_thread_event(thread_log, "🔄 Session is dead. Re-authenticating...", log_prefix)
            new_state = await login_and_get_session(session, user, log_prefix, thread_log)

            if new_state:
                session_state.update(new_state)
                log_thread_event(thread_log, "✅ Re-login successful. Updated global session_state.", log_prefix)
            else:
                log_thread_event(thread_log, "🚫 Re-login failed after all attempts.", log_prefix)

    except Exception as err:
        log_thread_event(thread_log, f"⚠️ [KEEP-ALIVE] Heartbeat request failed: {err}. Retrying next interval.",
                         log_prefix)

async def prepare_n_captchas(
        n: int,
        log_prefix: str,
        thread_log: list,
        proxy_string: str,
        base_url: str,
        sitekey: str
) -> list[str]:
    CAPTCHA_TIMEOUT_MS = 30 * 1000

    # Парсимо проксі (наприклад, "socks5://user:pass@ip:port")
    p = urlparse(proxy_string)

    # Стандартні параметри Chrome 146
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    client_hints = {
        "Sec-CH-UA": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"macOS"'
    }

    async def solve_single_captcha(index: int) -> str | None:
        id = index + 1
        async with aiohttp.ClientSession() as session:
            try:
                # 1. Створення завдання
                payload = {
                    "clientKey": CONFIG["CAPSOLVER_KEY"],
                    "task": {
                        "type": "AntiTurnstileTask",
                        "websiteURL": base_url,
                        "websiteKey": sitekey,
                        "proxyType": "socks5",
                        "proxyAddress": p.hostname,
                        "proxyPort": p.port,
                        "proxyLogin": p.username,
                        "proxyPassword": p.password,
                        "userAgent": user_agent,
                        "headers": client_hints
                    }
                }

                async with session.post("https://api.capsolver.com/createTask", json=payload) as resp:
                    create_data = await resp.json()

                if create_data.get("errorId", 0) != 0:
                    raise Exception(create_data.get("errorDescription", "Unknown Capsolver error"))

                task_id = create_data["taskId"]

                # 2. Очікування результату (Polling)
                start = asyncio.get_event_loop().time() * 1000
                while (asyncio.get_event_loop().time() * 1000) - start < CAPTCHA_TIMEOUT_MS:
                    async with session.post(
                            "https://api.capsolver.com/getTaskResult",
                            json={"clientKey": CONFIG["CAPSOLVER_KEY"], "taskId": task_id}
                    ) as res_resp:
                        data = await res_resp.json()

                    status = data.get("status")
                    if status == "ready":
                        log_thread_event(thread_log, f"[CAPSOLVER] Потік #{id}: ✅ Токен отримано!", log_prefix)
                        return data["solution"]["token"]

                    if status == "failed":
                        raise Exception("Capsolver task failed")

                    await asyncio.sleep(2.0)  # Опитування кожні 2 сек

                raise Exception("Capsolver timeout")

            except Exception as e:
                log_thread_event(thread_log, f"[CAPSOLVER] Потік #{id} помилка: {e}", log_prefix)
                return None

    # Запускаємо всі потоки паралельно через asyncio.gather
    tasks = [solve_single_captcha(i) for i in range(n)]
    results = await asyncio.gather(*tasks)

    # Повертаємо лише успішно отримані токени
    return [token for token in results if token is not None]

def log_thread_event(thread_log: list, message: str, stdout_prefix: str = None):
    now = datetime.now()
    timestamp = now.strftime("%H:%M:%S") + f".{str(int(now.microsecond / 1000)).zfill(3)}"
    formatted_line = f"[{timestamp}] {message}"
    thread_log.append(formatted_line)
    if stdout_prefix:
        print(f"{stdout_prefix} {formatted_line}")


async def make_request(
    session,
    method: str,
    url: str,
    log_prefix: str = "",
    thread_log: list | None = None,
    **kwargs,
):
    kwargs.setdefault("timeout", 35.0)
    method = method.upper()
    start = time.perf_counter()

    try:
        if method == "GET":
            response = await session.get(url, **kwargs)
        elif method == "POST":
            response = await session.post(url, **kwargs)
        else:
            response = await session.request(method, url, **kwargs)
    except Exception as exc:
        elapsed = (time.perf_counter() - start) * 1000
        log_thread_event(
            thread_log,
            f"HTTP {method} exception after {elapsed:.1f} ms: "
            f"{type(exc).__name__}: {exc}",
            log_prefix,
        )
        raise

    return response

async def login_and_get_session(session: AsyncSession, user: dict, log_prefix: str, thread_log: list) -> dict:
    MAX_RETRIES = 6
    delay_sec = 3.0

    def extract_csrf(html_text: str) -> str | None:
        if not html_text:
            return None
        soup = BeautifulSoup(html_text, "lxml")
        token_input = soup.find("input", {"name": "_csrf"})
        return token_input.get("value") if token_input else None

    def get_all_cookies() -> tuple[str, str, str]:
        try:
            cookie_dict = session.cookies.get_dict()
        except AttributeError:
            cookie_dict = dict(session.cookies) if session.cookies else {}
        bunny_id = next((val for name, val in cookie_dict.items() if name.startswith("bunny_shield_id")), "")

        return (
            cookie_dict.get("osCsid", ""),
            bunny_id,
            cookie_dict.get("cf_clearance", "")
        )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await make_request(
                session=session,
                method="GET",
                url=f"{CONFIG['BASE_URL']}/login.php",
                log_prefix=log_prefix,
                thread_log=thread_log,
                allow_redirects=True,
                timeout=10.0,
            )

            sleeptime = random.uniform(0.7, 1.2)
            await asyncio.sleep(sleeptime)

            response_text = resp.text if resp else ""

            if not response_text:
                raise Exception("⚠️ Empty page returned during login process.")

            csrf_token = extract_csrf(response_text)
            if not csrf_token:
                with open(f"login_failed_{attempt}_{log_prefix}.html", "w", encoding="utf-8") as f:
                    f.write(response_text)

                log_thread_event(
                    thread_log,
                    f"Status={resp.status_code}, URL={resp.url}, Content-Type={resp.headers.get('Content-Type')}",
                    log_prefix,
                )
                raise Exception("⚠️Failed to parse fresh CSRF token.")

            full_login = await make_request(
                session=session,
                method="POST",
                url=f"{CONFIG['BASE_URL']}/login.php?action=process",
                log_prefix=log_prefix,
                thread_log=thread_log,
                data={"_csrf": csrf_token, "email_address": user["email"], "password": user["password"]},
                allow_redirects=True,
                headers={"origin": CONFIG["BASE_URL"], "referer": f"{CONFIG['BASE_URL']}/login.php",
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "same-origin",
                    "sec-fetch-user": "?1",},
            )

            login_text = full_login.text if full_login else ""

            if 'login-bankid.php' in login_text or 'необхідно авторизуватися за допомогою Системи BankID НБУ' in login_text:
                log_thread_event(thread_log, "🚫BankId required. Aborting the thread...", log_prefix)
                return {}

            if 'Помилка токену' in login_text:
                log_thread_event(thread_log, "⚠️ Csrf token error. Trying again...", log_prefix)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                continue

            if 'абули пароль' in login_text or 'пройти авторизацію' in login_text:
                log_thread_event(thread_log, "⚠️ Для доступу до особистого кабінету вам необхідно повторно пройти авторизацію, натиснувши посилання Забули пароль. У разі зміни пароля переконайтесь, що введені дані є коректними.. Trying again...", log_prefix)
                await asyncio.sleep(random.uniform(1.0, 1.5))
                continue

            success_markers = (
                "account_edit.php",
                "account_password.php",
                "my_coins.php",
                "logoff.php"
            )
            if any(marker in login_text for marker in success_markers):
                osCsid, bunnyShieldId, cf_clearance = get_all_cookies()

                if not cf_clearance:
                    log_thread_event(thread_log, "✅ No cf_clearance found. User was approved easily", log_prefix)

                auth_csrf_token = extract_csrf(login_text) or csrf_token

                return {
                    "osCsid": osCsid,
                    "csrfToken": auth_csrf_token,
                    "bunnyShieldId": bunnyShieldId,
                    "cf_clearance": cf_clearance
                }

            with open(f"login_failed_{attempt}_{log_prefix}_postLogin.html", "w", encoding="utf-8") as f:
                f.write(login_text)

            raise Exception("Login response received, but success markers not found.")


        except Exception as error:
            log_thread_event(thread_log, f"⚠️ Спроба авторизації {attempt} не вдалася: {error}", log_prefix)
            if attempt == MAX_RETRIES:
                raise error
            await asyncio.sleep(delay_sec)

    return {}

async def get_product_url(
    session,
    product_id: int,
    thread_log=None,
    log_prefix=""
) -> str:
    url = f"{CONFIG['BASE_URL']}/product_info.php?products_id={product_id}"
    # Використовуємо твою функцію з allow_redirects=True, щоб зловити фінальну адресу
    response = await make_request(
        session=session,
        method="GET",
        url=url,
        log_prefix=log_prefix,
        thread_log=thread_log,
        allow_redirects=True,
        headers={
            "referer": f"{CONFIG['BASE_URL']}/",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
        },
    )
    final_url = response.url


    print(f"ID #{product_id} веде на посилання: {final_url}")
    return final_url




async def execute_cart_injection(
        session,
        product_id: int,
        token_pool: list,
        thread_log: list,
        session_state: dict,
        log_prefix: str,
        user: dict,
        login_and_get_session_func,
        url: str,
        attempt: int = 1
) -> bool:
    """Виконує додавання товару в кошик (prepare_buy -> add_product) з можливістю повторних спроб."""

    MAX_INJECTION_ATTEMPTS = 5
    if attempt > MAX_INJECTION_ATTEMPTS:
        log_thread_event(thread_log,
                         f"❌ [FAIL][ID #{product_id}] Injection abandoned after {MAX_INJECTION_ATTEMPTS} attempts.",
                         log_prefix)
        return False

    if not token_pool:
        log_thread_event(thread_log, f"❌ [CRITICAL][ID #{product_id}] Token pool empty on attempt #{attempt}!",
                         log_prefix)
        return False

    headers_base = {
        "referer": url,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
        "origin": CONFIG['BASE_URL'],
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    post_start = asyncio.get_event_loop().time() * 1000
    log_thread_event(thread_log,
                     f"[POST][ID #{product_id}] Injecting cart payload (Attempt {attempt}/{MAX_INJECTION_ATTEMPTS})...",
                     log_prefix)

    try:
        # КРОК 1: Отримання buy_token через /product_info.php?action=prepare_buy
        buy_token = None
        max_prep_retries = 3

        for prep_attempt in range(1, max_prep_retries + 1):
            prep_resp = await make_request(
                session=session,
                method="POST",
                url=f"{CONFIG['BASE_URL']}/product_info.php?action=prepare_buy",
                log_prefix=log_prefix,
                thread_log=thread_log,
                allow_redirects=False,
                headers=headers_base,
                data={
                    '_csrf': session_state.get("csrfToken", ""),
                    'products_id': str(product_id),
                    'cart_quantity': '1'
                }
            )

            raw_text = prep_resp.text if (prep_resp and prep_resp.text) else ""

            # Парсинг JSON з відповіді
            try:
                prep_data = prep_resp.json() if hasattr(prep_resp, 'json') else json.loads(raw_text)
            except Exception:
                prep_data = {}

            if prep_data.get("success") and prep_data.get("buy_token"):
                buy_token = prep_data["buy_token"]
                break

            # Обробка лімітів або помилок на етапі prepare
            text_lower = raw_text.lower()
            if '429 помилка' in text_lower or 'дозволено запитів' in text_lower:
                log_thread_event(thread_log, f"⚠️ [PREPARE RATE LIMIT] Backing off 1000ms...", log_prefix)
                await asyncio.sleep(1)
            else:
                log_thread_event(thread_log,
                                 f"⚠️ [PREPARE FAILED][ID #{product_id}] Try {prep_attempt}/{max_prep_retries}: {raw_text[:200]}",
                                 log_prefix)
                await asyncio.sleep(1)

        # Якщо buy_token так і не отримано, перезапускаємо всю ін'єкцію
        if not buy_token:
            log_thread_event(thread_log, f"❌ [PREPARE ABANDONED][ID #{product_id}] Retrying full flow...", log_prefix)
            await asyncio.sleep(0.5)
            return await execute_cart_injection(
                session, product_id, token_pool, thread_log, session_state,
                log_prefix, user, login_and_get_session_func, url, attempt + 1
            )

        # КРОК 2: Фінальний POST запит add_product з токеном капчі
        current_token = token_pool.pop(0)  # Беремо токен безпосередньо перед використанням

        checkout_resp = await make_request(
            session=session,
            method="POST",
            url=f"{CONFIG['BASE_URL']}/product_info.php?action=add_product",
            log_prefix=log_prefix,
            thread_log=thread_log,
            allow_redirects=False,
            headers=headers_base,
            data={
                '_csrf': session_state.get("csrfToken", ""),
                'cart_quantity': '1',
                'products_id': str(product_id),
                'cf-turnstile-response': current_token,
                'token': current_token,
                'buy_token': buy_token,
            }
        )

        latency = int((asyncio.get_event_loop().time() * 1000) - post_start)
        body = (checkout_resp.text if (checkout_resp and checkout_resp.text) else "").lower()
        location_hdr = checkout_resp.headers.get('location', '').lower() if (
                    checkout_resp and checkout_resp.headers) else ""

        # Успішне додавання
        is_success = '"success":true' in body and 'processed' in body
        already_in_cart = '"success":true' in body and 'спроба повторно додати' in body
        has_errors = any(err in body for err in ['message', 'спробуйте ще раз', 'помилк', 'capcha', '429 помилка', 'озволено запитів'])

        if (is_success and not has_errors) or already_in_cart:
            msg = "The product is already in cart!" if already_in_cart else "Successfully injected into cart!"
            log_thread_event(thread_log, f"🎉 [SUCCESS][ID #{product_id}] {msg} Roundtrip: {latency}ms", log_prefix)
            return True

        # Обробка помилок та редиректів
        if '422.php' in body or '422.php' in location_hdr:
            log_thread_event(thread_log, f"⚠️ [SERVER OVERLOAD] 422 redirect. Delaying retry...", log_prefix)
            await asyncio.sleep(0.6)

        elif 'login.php' in body or 'login.php' in location_hdr:
            log_thread_event(thread_log, f"🔄 [SESSION EXPIRED] Re-authenticating...", log_prefix)
            try:
                fresh_session = await login_and_get_session_func(session, user, log_prefix, thread_log)
                if not fresh_session:
                    log_thread_event(thread_log, "🚫 [RE-AUTH FAIL] Could not refresh session.", log_prefix)
                    return False

                session_state.update(fresh_session)
                log_thread_event(thread_log, f"🔑 [RE-AUTH SUCCESS] Updated osCsid: {session_state.get('osCsid')}",
                                 log_prefix)
            except Exception as reauth_err:
                log_thread_event(thread_log, f"❌ [RE-AUTH ERROR] {reauth_err}", log_prefix)
                await asyncio.sleep(1.0)
        else:
            log_thread_event(thread_log, f"⚠️ [INJECTION ERROR] Response: {body}", log_prefix)
            await asyncio.sleep(0.6)

        # Рекурсивна наступна спроба
        return await execute_cart_injection(
            session, product_id, token_pool, thread_log, session_state,
            log_prefix, user, login_and_get_session_func, url, attempt + 1
        )

    except Exception as err:
        latency = int((asyncio.get_event_loop().time() * 1000) - post_start)
        log_thread_event(thread_log, f"💥 [SOCKET DROP][ID #{product_id}] Reset after {latency}ms: {err}", log_prefix)
        await asyncio.sleep(0.6)
        return await execute_cart_injection(
            session, product_id, token_pool, thread_log, session_state,
            log_prefix, user, login_and_get_session_func, url, attempt + 1
        )

async def get_best_proxies(
        proxies_list: list[str],
        target_url: str,
        timeout: float = 15.0,
) -> list[str]:
    async def test_single_proxy(proxy: str):
        start = time.perf_counter()

        try:
            async with AsyncSession(
                    proxies={
                        "http": proxy,
                        "https": proxy,
                    },
                    impersonate="chrome146",
                    timeout=timeout,
                    verify=True,
            ) as session:

                response = await session.get(
                    target_url,
                    allow_redirects=False,
                    headers={
                        "referer": "https://coins.bank.gov.ua/",
                        "sec-fetch-dest": "document",
                        "sec-fetch-mode": "navigate",
                        "sec-fetch-site": "same-origin",
                    },
                )

                latency = (time.perf_counter() - start) * 1000
                body = response.text or ""

                # Якщо статус 403 або спрацював Cloudflare — просто фіксуємо причину
                if response.status_code == 403 or any(
                        x in body for x in
                        ["cf-mitigated", "Just a moment", "Checking your browser", "Attention Required"]
                ):
                    return {
                        "proxy": proxy,
                        "status": "FAIL",
                        "reason": "HTTP 403 / Cloudflare Challenge (Блокування захистом)"
                    }

                if response.status_code == 429:
                    return {"proxy": proxy, "status": "FAIL", "reason": "HTTP 429 Too Many Requests (Rate limit)"}

                if response.status_code >= 400:
                    return {"proxy": proxy, "status": "FAIL", "reason": f"HTTP Error {response.status_code}"}

                # Якщо сторінка занадто коротка
                if len(body) < 3000:
                    return {"proxy": proxy, "status": "FAIL", "reason": f"Small Body Size ({len(body)} bytes < 3000)"}

                return {
                    "proxy": proxy,
                    "status": "OK",
                    "latency": latency,
                }

        except Exception as e:
            err_str = str(e).lower()
            if "timeout" in err_str or "timed out" in err_str:
                reason = "Timeout (Перевищено час очікування)"
            elif "connection" in err_str or "refused" in err_str:
                reason = "Connection Refused / Network Error"
            else:
                reason = f"Exception: {type(e).__name__} ({e})"

            return {"proxy": proxy, "status": "FAIL", "reason": reason}

    print(f"[PROXY-CHECKER] Testing {len(proxies_list)} proxies on target...")

    results = await asyncio.gather(
        *(test_single_proxy(proxy) for proxy in proxies_list)
    )

    valid = [r for r in results if r["status"] == "OK"]
    failed = [r for r in results if r["status"] == "FAIL"]

    valid.sort(key=lambda x: x["latency"])

    print(f"\n🟢 Passed: {len(valid)}/{len(proxies_list)}")
    print(f"🔴 Failed: {len(failed)}/{len(proxies_list)}\n")

    if valid:
        print("--- РОБОЧІ ПРОКСІ (за швидкістю) ---")
        for item in valid:
            print(f"  {item['latency']:7.1f} ms -> {item['proxy']}")

    if failed:
        print("\n--- ДЕТАЛІ ПРОБЛЕМ НЕПРАЦЮЮЧИХ ПРОКСІ ---")
        for item in failed:
            print(f"  ❌ {item['proxy']} => {item['reason']}")

    return [item["proxy"] for item in valid]

def assign_best_proxies_to_users(users: list[dict], sorted_proxies: list[str]) -> list[dict]:
    if not sorted_proxies:
        print("[WARNING] Список відсортованих проксі порожній! Залишаються старі проксі.")
        return users

    for i, user in enumerate(users):
        # Беремо проксі по черзі з відсортованого списку (найшвидші першим)
        # Якщо користувачів більше, ніж проксі, використовуємо залишок за модулем (%)
        chosen_proxy = sorted_proxies[i % len(sorted_proxies)]
        user["proxy"] = chosen_proxy
        print(f"🔗 [ASSIGN] Користувач {user['email']} закріплений за проксі: {chosen_proxy}")

    return users



def format_proxy(line: str) -> str:
    host, port, user, password = line.strip().split(":")
    return f"socks5://{user}:{password}@{host}:{port}"

BASE_DIR = Path(__file__).resolve().parent

# Повний шлях до файлу resedential.txt у тій самій папці
file_path = BASE_DIR /"other"/ "run"/"resedential.txt"

with open(file_path, "r", encoding="utf-8") as f:
    my_proxies = [format_proxy(line) for line in f if line.strip()]

async def run():
    print(
        "Before running the app check these things:\n"
        "1) time is set exactly\n"
        "2) You have chosen a correct mode\n"
        "3) Links are correct\n"
        "4) Check all change signs\n"
        "5) Test your application before running it\n"
        "6) All accounts are included!\n"
    )
    input("[PRESS ENTER TO START RESOURCE ALLOCATION]")


    PROXY_LIFESPAN_MS = 10 * 60 * 1000

    DROP_SIGNAL_EVENT = asyncio.Event()

    ms_until_release = (reload_startTime.timestamp() * 1000) - (datetime.now().timestamp() * 1000)

    if ms_until_release > PROXY_LIFESPAN_MS:
        excess_sleep_time = ms_until_release - PROXY_LIFESPAN_MS
        print(f"[⏰ TIME-LOCK] Потік засинає на {(excess_sleep_time / 1000 / 60):.1f} хв.")
        await asyncio.sleep(excess_sleep_time / 1000)

    print("testing proxies...")
    best_proxies = await get_best_proxies(my_proxies, target_url="https://coins.bank.gov.ua/-hersones-tavrijskij-u-phutljari/p-1016.html")

    users = load_users_from_json('other/run/users.json')
    print(f"Loaded {len(users)} users from users.json")
    per_user_logs = [{"email": u["email"], "logs": []} for u in users]

    for user in users:
        user["coinIds"] = CONFIG["CoinIds"]

    users = assign_best_proxies_to_users(users, best_proxies)

    ms_until_release = (reload_startTime.timestamp() * 1000) - (datetime.now().timestamp() * 1000)
    if ms_until_release > CAPTCHA_BUFFER_MS:
        excess_sleep_time = ms_until_release - CAPTCHA_BUFFER_MS
        print(f"[⏰ TIME-LOCK] Потік засинає на {(excess_sleep_time / 1000 / 60):.1f} хв.")
        await asyncio.sleep(excess_sleep_time / 1000)


    # Лічильник готових до бою акаунтів
    async def user_thread(index: int, user: dict):
        log_prefix = f"[User #{index + 1} - {user['email']}]"
        user_log = per_user_logs[index]["logs"]
        proxy_url = user.get("proxy")

        session_lock = asyncio.Lock()

        # 1. Підготовка капчі
        log_thread_event(user_log, "Preparing CAPTCHA tokens ahead of drop...", log_prefix)
        tokens = await prepare_n_captchas(
            n=CONFIG["NUMBER_OF_CAPTCHAS"],
            log_prefix=log_prefix,
            thread_log=user_log,
            proxy_string=proxy_url,
            base_url=CONFIG["BASE_URL"],
            sitekey=CONFIG["SITEKEY"]
        ) or []

        async with AsyncSession(
                proxies={"http": proxy_url, "https": proxy_url},
                impersonate="chrome146",
                timeout=35.0,
                verify=True,
        ) as session:
            try:
                login_delay = (index * 150) + (random.random() * 500)
                await asyncio.sleep(login_delay / 1000)

                login_abort_time = reload_startTime - timedelta(seconds=5)
                log_thread_event(user_log, "Authenticating user credentials against backend...", log_prefix)
                session_state = await login_and_get_session(session, user, log_prefix, user_log)

                while not session_state:
                    if datetime.now() >= login_abort_time:
                        log_thread_event(user_log, "🚫 Too late for re-login attempt. Aborting...", log_prefix)
                        return

                    log_thread_event(user_log, "🚫 Login failed. Retrying in 5s...", log_prefix)
                    await asyncio.sleep(5)
                    session_state = await login_and_get_session(session, user, log_prefix, user_log)

                log_thread_event(user_log, "✅ Successfully authenticated! Session active.", log_prefix)

                urls = []
                for product_id in user["coinIds"]:
                    url = await get_product_url(session, product_id, thread_log=user_log, log_prefix=log_prefix)
                    if not url:
                        urls.append(f"{CONFIG['BASE_URL']}/product_info.php?products_id={product_id}")
                    else:
                        urls.append(url)

                await asyncio.sleep(2)


                # 2. Цикл очікування сигналу дропу (Keep-Alive)
                while not DROP_SIGNAL_EVENT.is_set():
                    try:
                        if datetime.now() >= reload_startTime:
                            await asyncio.wait_for(DROP_SIGNAL_EVENT.wait(), timeout=random.uniform(1.5, 3.0))
                        else:
                            await asyncio.wait_for(DROP_SIGNAL_EVENT.wait(), timeout=random.uniform(7.0, 10.0))
                    except asyncio.TimeoutError:
                        # Запускаємо фонову перевірку та передаємо session_lock
                        if (datetime.now() - reload_startTime).total_seconds() < 7:
                            asyncio.create_task(
                                keep_alive_and_verify(
                                    session=session,
                                    thread_log=user_log,
                                    log_prefix=log_prefix,
                                    user=user,
                                    session_state=session_state,
                                    url=urls[0],
                                    session_lock=session_lock
                                )
                            )

                # 3. МИТТЄВА КУПІВЛЯ
                log_thread_event(user_log, "🚀 DROP SIGNAL RECEIVED! Executing instant buy...", log_prefix)

                async def run_injection_with_offset(i, product_id):
                    if i > 0:
                        await asyncio.sleep(i * 0.25)

                    return await execute_cart_injection(
                        session=session,
                        product_id=product_id,
                        token_pool=tokens,
                        thread_log=user_log,
                        session_state=session_state,
                        log_prefix=log_prefix,
                        user=user,
                        login_and_get_session_func=login_and_get_session,
                        url=urls[i],
                        attempt=1
                    )

                results = await asyncio.gather(*[
                    run_injection_with_offset(i, product_id)
                    for i, product_id in enumerate(user["coinIds"])
                ])

                for product_id, success in zip(user["coinIds"], results):
                    log_thread_event(user_log, f"🎯 Buy result for coin #{product_id}: {success}", log_prefix)

            except Exception as e:
                print(f"{log_prefix} Error: {e}")

    print(f"[SYSTEM] Launching {len(users)} user threads (Login & Keep-Alive)...")
    user_tasks = [asyncio.create_task(user_thread(i, u)) for i, u in enumerate(users)]

    # ------------------------------------------------------------------
    # ЗАПУСК АВТОНОМНОГО МОНІТОРА (High-Speed Scanner)
    # ------------------------------------------------------------------
    BASE_DIR = Path(__file__).resolve().parent
    file_path = BASE_DIR /"other"/ "run"/"datacenter.txt"

    with open(file_path, "r", encoding="utf-8") as f:
        monitor_proxies = f.read().splitlines()

    monitor = CoinDropMonitor(
        target_url=CONFIG['FirstCoinUrl'],
        drop_time=reload_startTime,
        proxies=monitor_proxies,
        interval_ms=CONFIG['MonitorIntervalMS'],
        max_users=100
    )

    print("🚀 [SYSTEM] High-Speed Monitor is now active...")
    monitor_result = await monitor.start()

    if monitor_result.success:
        print("🔥 [SYSTEM] BUTTON DETECTED! Triggering all users to BUY immediately!")
        DROP_SIGNAL_EVENT.set()
    else:
        print("❌ [SYSTEM] Monitor failed to detect status.")

    await asyncio.gather(*user_tasks)


if __name__ == "__main__":
    asyncio.run(run())