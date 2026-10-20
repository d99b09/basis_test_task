import asyncio
import sys
from threading import Event
from types import SimpleNamespace

import pytest
from integration.cleanup import pause_before_cleanup


def make_request(*, enabled=True, load=False):
    return SimpleNamespace(
        config=SimpleNamespace(
            getoption=lambda name: enabled,
            pluginmanager=SimpleNamespace(getplugin=lambda name: None),
        ),
        fixturenames=["payment_load_control"] if load else [],
        node=SimpleNamespace(nodeid="inspection-test"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("load", [False, True])
async def test_automated_runs_never_read_stdin(monkeypatch, load):
    def unexpected_input(prompt):
        pytest.fail("Automated runs must not wait for user input")

    monkeypatch.setattr("builtins.input", unexpected_input)
    await pause_before_cleanup(make_request(enabled=False, load=load))


@pytest.mark.asyncio
@pytest.mark.parametrize("load", [False, True])
async def test_pause_keeps_event_loop_running_until_enter(monkeypatch, capsys, load):
    waiting = Event()
    enter = Event()

    def read_enter(prompt):
        waiting.set()
        assert enter.wait(timeout=5), "Test did not release input"
        return ""

    monkeypatch.setattr("builtins.input", read_enter)
    task = asyncio.create_task(
        pause_before_cleanup(make_request(load=load), "Схема БД: inspection_schema")
    )
    try:
        assert await asyncio.to_thread(waiting.wait, 2)
        # The event loop can serve other work while the input thread is waiting.
        await asyncio.wait_for(asyncio.sleep(0), timeout=1)
        assert not task.done()
        assert "inspection_schema" in capsys.readouterr().out
    finally:
        enter.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_pause_temporarily_disables_pytest_capture(monkeypatch, pytestconfig):
    request = make_request()
    request.config.pluginmanager = pytestconfig.pluginmanager
    stdin_before, stdout_before = sys.stdin, sys.stdout

    def read_enter(prompt):
        # Pytest's captured stdin raises even for a zero-length read.
        assert sys.stdin.read(0) == ""
        return ""

    monkeypatch.setattr("builtins.input", read_enter)
    await pause_before_cleanup(request)
    assert sys.stdin is stdin_before
    assert sys.stdout is stdout_before


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [EOFError, OSError])
async def test_pause_reports_missing_interactive_input(monkeypatch, error):
    def unavailable_input(prompt):
        raise error()

    monkeypatch.setattr("builtins.input", unavailable_input)
    with pytest.raises(pytest.fail.Exception, match="интерактивный ввод"):
        await pause_before_cleanup(make_request())
