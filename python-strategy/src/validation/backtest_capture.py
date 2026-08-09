"""Pure event-time projections for backtest parity evidence."""

from src.core.models import Signal, SignalType
from src.validation.trading_outcome import SignalObservation

__all__ = ["capture_signal_batch"]

_FIELD_NAMES = frozenset(Signal.model_fields)
_RENAMED_FIELDS = {
    "timestamp": "timestamp_ms",
    "type": "signal_type",
    "metadata": "metadata_json",
}


def capture_signal_batch(signals: object) -> tuple[SignalObservation, ...]:
    """Freeze one finalized signal batch at the observer call boundary."""
    if type(signals) is not tuple:
        raise ValueError("signal capture requires an exact tuple")

    captured: list[SignalObservation] = []
    for signal in signals:
        if type(signal) is not Signal:
            raise ValueError("signal capture requires exact Signal instances")
        values = dict(signal.__dict__)
        if signal.model_extra is not None or set(values) != _FIELD_NAMES:
            raise ValueError("Signal contains missing or unexpected fields")
        signal_type = values["type"]
        if type(signal_type) is not SignalType:
            raise ValueError("Signal type must be an exact SignalType")
        values["type"] = signal_type.value
        captured.append(
            SignalObservation.model_validate(
                {
                    _RENAMED_FIELDS.get(name, name): value
                    for name, value in values.items()
                }
            )
        )
    return tuple(captured)
