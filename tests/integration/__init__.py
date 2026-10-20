import pytest

pytest.register_assert_rewrite(f"{__name__}.live_stack")
pytest.register_assert_rewrite(f"{__name__}.docker_control")
pytest.register_assert_rewrite(f"{__name__}.rabbitmq_stack")
