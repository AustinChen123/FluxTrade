import logging
from collections.abc import Callable
from typing import Protocol


class _JoinableThread(Protocol):
    def is_alive(self) -> bool: ...

    def join(self, *, timeout: float) -> None: ...


class EngineShutdownService:
    """Run StrategyEngine's existing ordered shutdown transaction."""

    def __init__(
        self,
        *,
        mark_stopping: Callable[[], None],
        shutdown_executor: Callable[[], None],
        thread_loaders: tuple[Callable[[], _JoinableThread | None], ...],
        adapter_loader: Callable[[], object],
        shutdown_strategy_state: Callable[[], None],
        clean_persistence_allowed: Callable[[], bool],
        persist_clean: Callable[[], object],
        close_redis: Callable[[], None],
        close_entry_gate: Callable[[], None],
        event_logger: logging.Logger,
    ) -> None:
        self._mark_stopping = mark_stopping
        self._shutdown_executor = shutdown_executor
        self._thread_loaders = thread_loaders
        self._adapter_loader = adapter_loader
        self._shutdown_strategy_state = shutdown_strategy_state
        self._clean_persistence_allowed = clean_persistence_allowed
        self._persist_clean = persist_clean
        self._close_redis = close_redis
        self._close_entry_gate = close_entry_gate
        self._logger = event_logger

    def shutdown(self, *, timeout: float, clean_exit: bool) -> None:
        self._logger.info("StrategyEngine shutting down...")
        self._mark_stopping()
        self._shutdown_executor()

        for load_thread in self._thread_loaders:
            thread = load_thread()
            if thread is not None and thread.is_alive():
                thread.join(timeout=timeout)

        self._close_adapter()
        self._shutdown_strategy_state()

        if clean_exit and self._clean_persistence_allowed():
            self._persist_clean()

        try:
            self._close_redis()
        except Exception as exc:
            self._logger.warning("Error closing Redis: %s", exc)

        self._close_entry_gate()
        self._logger.info("StrategyEngine shutdown complete.")

    def _close_adapter(self) -> None:
        close_adapter = getattr(self._adapter_loader(), "close", None)
        if callable(close_adapter):
            close_adapter()
