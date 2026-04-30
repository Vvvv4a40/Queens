import asyncio
import logging
from urllib.parse import quote
from playwright.async_api import async_playwright, Browser, Page
from config import CHAT_EXIT_MESSAGE_THRESHOLD, CHAT_MONITOR_INTERVAL_SECONDS, HEADLESS

log = logging.getLogger(__name__)

# ── Хранилище активных сессий: {telegram_id: Page} ──────────
_sessions: dict[int, Page]    = {}
_contexts: dict[int, object]  = {}
_monitor_tasks: dict[int, asyncio.Task] = {}
_browser:  Browser | None     = None
_pw                           = None
_auto_disconnect_callback     = None


def set_auto_disconnect_callback(callback):
    global _auto_disconnect_callback
    _auto_disconnect_callback = callback


async def start():
    """Запускает Playwright. Вызвать один раз при старте бота."""
    global _browser, _pw
    _pw      = await async_playwright().start()
    _browser = await _pw.chromium.launch(
        headless=HEADLESS,
        args=["--no-sandbox", "--disable-gpu",
              "--disable-dev-shm-usage", "--mute-audio",
              "--autoplay-policy=no-user-gesture-required",
              "--use-fake-ui-for-media-stream",
              "--use-fake-device-for-media-stream"]
    )
    log.info("Браузер запущен (headless=%s)", HEADLESS)


async def stop():
    """Останавливает браузер при выключении бота."""
    for uid in list(_sessions):
        await disconnect(uid)
    if _browser:
        await _browser.close()
    if _pw:
        await _pw.stop()


def build_url(eclass_url: str, full_name: str) -> str:
    """
    Вставляет ФИО в ссылку e-class.

    e-class принимает ФИО прямо в URL:
    https://e-class.tsu.ru/#join:ROOM_ID,true,ФИО_в_URL_кодировке

    Если пользователь уже прислал ссылку с чужим именем — мы
    вырезаем имя и вставляем своё.
    Если ссылка без имени — просто добавляем.
    """
    # Разбиваем по запятой: [join:ROOM_ID, true, ИМЯ(если есть)]
    if "#join:" in eclass_url:
        parts = eclass_url.split(",")
        if len(parts) >= 2:
            base = parts[0] + "," + parts[1]
            return f"{base},{quote(full_name, safe='')}"

    # Ссылка в другом формате — отдаём как есть
    return eclass_url


async def connect(telegram_id: int, url: str, full_name: str) -> bool:
    """
    Открывает вкладку браузера и входит в лекцию.
    Возвращает True если успешно.
    """
    if telegram_id in _sessions:
        log.info("Пользователь %s уже в сессии, переключаем на новую лекцию", telegram_id)
        await disconnect(telegram_id)
    if not _browser:
        log.error("Браузер не запущен!")
        return False

    try:
        join_url = build_url(url, full_name)
        log.info("Подключаем %s → %s", telegram_id, join_url[:80])

        ctx  = await _browser.new_context(
            ignore_https_errors=True,
            permissions=["microphone", "camera"],
        )
        page = await ctx.new_page()

        # Блокируем картинки — экономим RAM
        await ctx.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2}",
                        lambda r: r.abort())

        await page.goto(join_url, wait_until="domcontentloaded", timeout=30_000)

        # Заполняем имя гостя и жмём кнопку входа, если сайт показывает форму.
        await _join_as_guest(page, full_name)

        _sessions[telegram_id] = page
        _contexts[telegram_id] = ctx
        _start_chat_monitor(telegram_id, page)
        log.info("✅ %s подключён", telegram_id)
        return True

    except Exception as e:
        log.error("Ошибка подключения %s: %s", telegram_id, e)
        return False


async def disconnect(telegram_id: int) -> bool:
    """Закрывает вкладку — выходит из лекции."""
    task = _monitor_tasks.pop(telegram_id, None)
    if task:
        task.cancel()
    page = _sessions.pop(telegram_id, None)
    ctx  = _contexts.pop(telegram_id, None)
    if not page:
        return False
    try:
        if not page.is_closed():
            await page.close()
        if ctx:
            await ctx.close()
        log.info("👋 %s отключён", telegram_id)
        return True
    except Exception as e:
        log.error("Ошибка отключения %s: %s", telegram_id, e)
        return False


def is_connected(telegram_id: int) -> bool:
    return telegram_id in _sessions


async def _join_as_guest(page: Page, full_name: str):
    """Пробует заполнить ФИО и нажать кнопку входа."""
    await asyncio.sleep(2)
    await _fill_guest_name(page, full_name)

    for _ in range(3):
        if await _click_join(page):
            await asyncio.sleep(2)
            await _click_audio_only(page)
            return
        await asyncio.sleep(1)
        await _fill_guest_name(page, full_name)


async def _fill_guest_name(page: Page, full_name: str):
    for sel in [
        "input[name='name']",
        "input[name='username']",
        "input[id*='name' i]",
        "input[placeholder*='ФИО' i]",
        "input[placeholder*='Имя' i]",
        "input[placeholder*='Name' i]",
        "input[type='text']",
    ]:
        try:
            field = await page.wait_for_selector(sel, timeout=800, state="visible")
            if field:
                value = await field.input_value()
                if not value.strip():
                    await field.fill(full_name)
                    log.info("Заполнено ФИО гостя через %s", sel)
                return
        except Exception:
            continue


async def _click_join(page: Page) -> bool:
    """Пробует найти и нажать кнопку входа."""
    for sel in [
        "button:has-text('Войти')",
        "button:has-text('Подключиться')",
        "button:has-text('Присоединиться')",
        "button:has-text('Войти как гость')",
        "button:has-text('Продолжить')",
        "button:has-text('Join')",
        "button:has-text('Join meeting')",
        "button:has-text('Continue')",
        "input[type='submit']",
        "[class*='join']",
    ]:
        try:
            btn = await page.wait_for_selector(sel, timeout=2_000, state="visible")
            if btn:
                await btn.click()
                log.info("Нажата кнопка: %s", sel)
                return True
        except Exception:
            continue
    return False


async def _click_audio_only(page: Page):
    for sel in [
        "button:has-text('Слушать')",
        "button:has-text('Только слушать')",
        "button:has-text('Listen only')",
        "button:has-text('Microphone')",
    ]:
        try:
            btn = await page.wait_for_selector(sel, timeout=1_500, state="visible")
            if btn:
                await btn.click()
                log.info("Нажата аудио-кнопка: %s", sel)
                return
        except Exception:
            continue


def _start_chat_monitor(telegram_id: int, page: Page):
    if CHAT_EXIT_MESSAGE_THRESHOLD <= 0:
        return
    task = _monitor_tasks.pop(telegram_id, None)
    if task:
        task.cancel()
    _monitor_tasks[telegram_id] = asyncio.create_task(_monitor_chat_and_leave(telegram_id, page))


async def _monitor_chat_and_leave(telegram_id: int, page: Page):
    await asyncio.sleep(8)
    await _try_open_chat(page)
    while telegram_id in _sessions and not page.is_closed():
        try:
            count = await _count_chat_messages(page)
            if count >= CHAT_EXIT_MESSAGE_THRESHOLD:
                log.info(
                    "В чате %s сообщений, отключаем %s досрочно",
                    count,
                    telegram_id,
                )
                await disconnect(telegram_id)
                if _auto_disconnect_callback:
                    await _auto_disconnect_callback(telegram_id, count)
                return
        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.debug("Не удалось проверить чат для %s: %s", telegram_id, exc)
        await asyncio.sleep(CHAT_MONITOR_INTERVAL_SECONDS)


async def _try_open_chat(page: Page):
    for sel in [
        "button:has-text('Чат')",
        "button:has-text('Общий чат')",
        "button:has-text('Public Chat')",
        "button[aria-label*='чат' i]",
        "button[aria-label*='chat' i]",
        "[data-test*='chat' i]",
    ]:
        try:
            btn = await page.wait_for_selector(sel, timeout=900, state="visible")
            if btn:
                await btn.click()
                await asyncio.sleep(1)
                return
        except Exception:
            continue


async def _count_chat_messages(page: Page) -> int:
    return await page.evaluate(
        """
        () => {
          const selectors = [
            '[data-test*="chat"] [data-test*="message"]',
            '[class*="chat"] [class*="message"]',
            '[class*="Chat"] [class*="Message"]',
            '[aria-label*="chat" i] [role="listitem"]',
            '[aria-label*="чат" i] [role="listitem"]',
            '[role="log"] [role="listitem"]'
          ];
          const seen = new Set();
          for (const selector of selectors) {
            document.querySelectorAll(selector).forEach((node) => {
              const rect = node.getBoundingClientRect();
              const text = (node.innerText || node.textContent || '').trim();
              if (rect.width > 0 && rect.height > 0 && text.length > 0) {
                seen.add(text.slice(0, 160));
              }
            });
          }
          return seen.size;
        }
        """
    )
