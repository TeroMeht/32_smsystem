"""
Service alarms layer.

Sits on top of the datapipe: the dispatcher registers as the
``BarSink`` that ``process_bar`` invokes for every enriched
``CandleRow``. Each registered strategy inspects the bar and, when
it fires, ``alarm_generator.generate_signal_alarm`` handles dedupe
and Telegram delivery.

Entry point: ``dispatcher.build_alarm_sink(settings)`` -> BarSink.
Wired in ``backend.main.lifespan`` and handed to
``pipeline.startup(app, pool, polygon, sink=...)``.
"""
