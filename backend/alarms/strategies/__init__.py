"""
Alarm strategies -- one predicate per file, one signal name per module.

Each strategy exposes:

    SIGNAL_NAME: str
    async def run(bar: CandleRow) -> None

``run`` inspects the enriched bar, decides whether the setup fires,
and delegates dedupe + Telegram to ``alarm_generator.generate_signal_alarm``.

New alarms are added by dropping a new file here and appending its
``run`` to ``dispatcher._STRATEGIES``.
"""
