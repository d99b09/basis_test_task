import asyncio
from contextlib import nullcontext

import pytest


async def wait_for_enter(request, prompt, *details):
    capture = request.config.pluginmanager.getplugin("capturemanager")
    with capture.global_and_fixture_disabled() if capture else nullcontext():
        if capture:
            capture.suspend_global_capture(in_=True)
        for detail in details:
            print(detail, flush=True)
        try:
            await asyncio.to_thread(input, prompt)
        except (EOFError, OSError):
            pytest.fail(
                "Для продолжения нужен интерактивный ввод. Запустите тест в терминале."
            )
