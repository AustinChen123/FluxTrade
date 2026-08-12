"""Redis strategy-control listener lifecycle."""

import json
import logging
import threading
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


def build_strategy_command_listener(
    *,
    pubsub_factory: Callable[[], Any],
    is_running: Callable[[], bool],
    assert_leadership: Callable[[], None],
    submit_command: Callable[[dict[str, object]], object],
    event_logger: logging.Logger = logger,
) -> threading.Thread:
    """Build the strategy-control subscriber daemon thread."""

    def command_loop() -> None:
        pubsub = pubsub_factory()
        try:
            pubsub.subscribe("cmd:strategy:control")
            event_logger.info(
                "📡 Command Listener Started. Subscribed to 'cmd:strategy:control'"
            )
            while is_running():
                message = pubsub.get_message(timeout=1.0)
                if message is None or message["type"] != "message":
                    continue
                try:
                    assert_leadership()
                except Exception:
                    return
                try:
                    data = json.loads(message["data"])
                    if not isinstance(data, dict):
                        raise ValueError("command payload must be a JSON object")
                    submit_command(data)
                except Exception as error:
                    event_logger.error("Error parsing command: %s", error)
        finally:
            pubsub.close()

    thread = threading.Thread(target=command_loop, daemon=True)
    return thread
