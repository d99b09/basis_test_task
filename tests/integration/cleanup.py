from .console import wait_for_enter


async def pause_before_cleanup(request, *details):
    if not request.config.getoption("--pause-before-cleanup"):
        return
    await wait_for_enter(
        request,
        "Нажмите Enter для продолжения и очистки данных... ",
        f"\nПауза перед очисткой: {request.node.nodeid}",
        *details,
    )
