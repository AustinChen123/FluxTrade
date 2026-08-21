"""Provider-neutral Engine heartbeat worker lifecycle."""

import logging
import threading
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class EngineHeartbeatService:
    """Run process and strategy heartbeats in one daemon worker."""

    def __init__(
        self,
        *,
        is_running: Callable[[], bool],
        assert_leadership: Callable[[], None],
        write_process_heartbeat: Callable[[], object],
        observe_entry_admission: Callable[[], bool],
        record_balance_metric: Callable[[], None],
        load_active_strategy_ids: Callable[[], list[str]],
        record_strategy_heartbeats: Callable[[list[str]], None],
        sleep: Callable[[float], None] = time.sleep,
        event_logger: logging.Logger = logger,
    ) -> None:
        self._is_running = is_running
        self._assert_leadership = assert_leadership
        self._write_process_heartbeat = write_process_heartbeat
        self._observe_entry_admission = observe_entry_admission
        self._record_balance_metric = record_balance_metric
        self._load_active_strategy_ids = load_active_strategy_ids
        self._record_strategy_heartbeats = record_strategy_heartbeats
        self._sleep = sleep
        self._logger = event_logger
        self.thread: threading.Thread | None = None

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self._run, daemon=True)
        self.thread = thread
        thread.start()
        return thread

    def _run(self) -> None:
        self._logger.info("💓 Heartbeat Service Started.")
        while self._is_running():
            try:
                self._assert_leadership()
            except Exception:
                return
            try:
                self._write_process_heartbeat()
                strategy_heartbeat_allowed = self._observe_entry_admission()
                try:
                    self._record_balance_metric()
                except Exception:
                    pass
                active_strategy_ids = self._load_active_strategy_ids()
                if strategy_heartbeat_allowed:
                    self._record_strategy_heartbeats(active_strategy_ids)
                self._sleep(1.0)
            except Exception as error:
                self._logger.error("💓 Heartbeat Failed: %s", error)
                self._sleep(1.0)
