import 'dart:async';

abstract interface class Clock {
  DateTime now();
}

final class SystemClock implements Clock {
  const SystemClock();
  @override
  DateTime now() => DateTime.now();
}

abstract interface class TimerFactory {
  Timer once(Duration delay, void Function() callback);
  Timer periodic(Duration interval, void Function(Timer timer) callback);
}

final class DartTimerFactory implements TimerFactory {
  const DartTimerFactory();
  @override
  Timer once(Duration delay, void Function() callback) =>
      Timer(delay, callback);
  @override
  Timer periodic(Duration interval, void Function(Timer timer) callback) =>
      Timer.periodic(interval, callback);
}
