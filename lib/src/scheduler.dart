import 'dart:async';

import 'clock.dart';
import 'persistence.dart';

enum Recurrence { once, daily, weekly, weekdays, customWeekdays }

enum ScheduleAction { startQueue, startTask, pauseAll, custom }

enum ScheduleEventKind { fired, skipped, missed }

final class ScheduleWindow {
  const ScheduleWindow({required this.startMinute, required this.endMinute});

  /// Minute of local day, 0..1439.
  final int startMinute;
  final int endMinute;
  Map<String, dynamic> toJson() => {
    'startMinute': startMinute,
    'endMinute': endMinute,
  };
  factory ScheduleWindow.fromJson(Map<String, dynamic> json) => ScheduleWindow(
    startMinute: (json['startMinute'] as num).toInt(),
    endMinute: (json['endMinute'] as num).toInt(),
  );
}

final class ScheduleEntry {
  ScheduleEntry({
    required this.id,
    required this.label,
    required this.fireTime,
    this.recurrence = Recurrence.once,
    this.window,
    this.assignedQueueId,
    this.action = ScheduleAction.custom,
    this.taskId,
    Set<int> weekdays = const {},
    this.actionName,
    this.callback,
  }) : weekdays = Set.unmodifiable(weekdays) {
    if (id.isEmpty) throw ArgumentError.value(id, 'id');
    if (this.weekdays.any((d) => d < 1 || d > 7)) {
      throw ArgumentError.value(
        weekdays,
        'weekdays',
        'use DateTime weekdays 1..7',
      );
    }
  }
  final String id;
  final String label;
  final DateTime fireTime;
  final Recurrence recurrence;
  final ScheduleWindow? window;
  final String? assignedQueueId;
  final ScheduleAction action;
  final String? taskId;
  final Set<int> weekdays;

  /// Host-defined callback key (e.g. `shutdownSystem` or `sleepSystem`).
  final String? actionName;

  /// In-memory callback; the host must rebind after restoring persisted data.
  final FutureOr<void> Function()? callback;

  ScheduleEntry copyWith({
    DateTime? fireTime,
    FutureOr<void> Function()? callback,
  }) => ScheduleEntry(
    id: id,
    label: label,
    fireTime: fireTime ?? this.fireTime,
    recurrence: recurrence,
    window: window,
    assignedQueueId: assignedQueueId,
    action: action,
    taskId: taskId,
    weekdays: weekdays,
    actionName: actionName,
    callback: callback ?? this.callback,
  );

  Map<String, dynamic> toJson() => {
    'id': id,
    'label': label,
    'fireTime': fireTime.toIso8601String(),
    'recurrence': recurrence.name,
    'window': window?.toJson(),
    'assignedQueueId': assignedQueueId,
    'action': action.name,
    'taskId': taskId,
    'weekdays': weekdays.toList(),
    'actionName': actionName,
  };

  factory ScheduleEntry.fromJson(Map<String, dynamic> json) => ScheduleEntry(
    id: json['id'] as String,
    label: json['label'] as String? ?? '',
    fireTime: DateTime.parse(json['fireTime'] as String),
    recurrence: Recurrence.values.firstWhere(
      (v) => v.name == json['recurrence'],
      orElse: () => Recurrence.once,
    ),
    window: json['window'] is Map
        ? ScheduleWindow.fromJson(
            (json['window'] as Map).cast<String, dynamic>(),
          )
        : null,
    assignedQueueId: json['assignedQueueId'] as String?,
    action: ScheduleAction.values.firstWhere(
      (v) => v.name == json['action'],
      orElse: () => ScheduleAction.custom,
    ),
    taskId: json['taskId'] as String?,
    weekdays: ((json['weekdays'] as List?) ?? const [])
        .whereType<num>()
        .map((n) => n.toInt())
        .toSet(),
    actionName: json['actionName'] as String?,
  );

  DateTime? nextAfter(DateTime after) {
    if (recurrence == Recurrence.once)
      return fireTime.isAfter(after) ? fireTime : null;
    final firstDay = DateTime(after.year, after.month, after.day);
    for (var i = 0; i <= 370; i++) {
      final day = firstDay.add(Duration(days: i));
      final matches = switch (recurrence) {
        Recurrence.once => false,
        Recurrence.daily => true,
        Recurrence.weekly => day.weekday == fireTime.weekday,
        Recurrence.weekdays =>
          day.weekday >= DateTime.monday && day.weekday <= DateTime.friday,
        Recurrence.customWeekdays => weekdays.contains(day.weekday),
      };
      if (!matches) continue;
      var candidate = DateTime(
        day.year,
        day.month,
        day.day,
        fireTime.hour,
        fireTime.minute,
        fireTime.second,
        fireTime.millisecond,
      );
      final w = window;
      if (w != null &&
          !_insideWindow(candidate.hour * 60 + candidate.minute, w)) {
        candidate = DateTime(
          day.year,
          day.month,
          day.day,
          w.startMinute ~/ 60,
          w.startMinute % 60,
          fireTime.second,
        );
      }
      if (candidate.isAfter(after)) return candidate;
    }
    return null;
  }

  static bool _insideWindow(int minute, ScheduleWindow w) =>
      w.startMinute <= w.endMinute
      ? minute >= w.startMinute && minute <= w.endMinute
      : minute >= w.startMinute || minute <= w.endMinute;
}

final class ScheduleEvent {
  const ScheduleEvent({
    required this.entry,
    required this.kind,
    required this.at,
    this.error,
  });
  final ScheduleEntry entry;
  final ScheduleEventKind kind;
  final DateTime at;
  final Object? error;
}

typedef ScheduleActionHandler = FutureOr<void> Function(ScheduleEntry entry);

/// Timer-driven dispatcher. Recurring entries persist their next fire time;
/// expired entries are reported on restore rather than silently run late.
final class TaskScheduler {
  TaskScheduler({
    Clock clock = const SystemClock(),
    TimerFactory timers = const DartTimerFactory(),
    this.onStartQueue,
    this.onStartTask,
    this.onPauseAll,
    this.onCustomAction,
    this.onPersist,
    this.store,
  }) : _clock = clock,
       _timers = timers;

  final Clock _clock;
  final TimerFactory _timers;
  final ScheduleActionHandler? onStartQueue;
  final ScheduleActionHandler? onStartTask;
  final ScheduleActionHandler? onPauseAll;
  final ScheduleActionHandler? onCustomAction;
  final Future<void> Function(List<ScheduleEntry>)? onPersist;
  final QueueSchedulerStore? store;
  final Map<String, ScheduleEntry> _entries = {};
  final _events = StreamController<ScheduleEvent>.broadcast();
  Timer? _timer;
  bool _disposed = false;
  Stream<ScheduleEvent> get events => _events.stream;
  List<ScheduleEntry> get entries => List.unmodifiable(_entries.values);

  Future<void> restore([Iterable<ScheduleEntry>? pending]) async {
    pending ??= (await store?.load())?.entries ?? const <ScheduleEntry>[];
    _entries
      ..clear()
      ..addEntries(pending.map((e) => MapEntry(e.id, e)));
    final now = _clock.now();
    for (final entry in List.of(_entries.values)) {
      if (!entry.fireTime.isBefore(now)) continue;
      _emit(entry, ScheduleEventKind.missed, now);
      final next = entry.nextAfter(now);
      if (next == null) {
        _entries.remove(entry.id);
      } else {
        _entries[entry.id] = entry.copyWith(fireTime: next);
      }
    }
    await _persist();
    _arm();
  }

  Future<void> add(ScheduleEntry entry) async {
    _ensureAlive();
    _entries[entry.id] = entry;
    await _persist();
    _arm();
  }

  Future<void> remove(String id) async {
    _entries.remove(id);
    await _persist();
    _arm();
  }

  void _arm() {
    _timer?.cancel();
    if (_disposed || _entries.isEmpty) return;
    final next = _entries.values
        .map((e) => e.fireTime)
        .reduce((a, b) => a.isBefore(b) ? a : b);
    final delay = next.difference(_clock.now());
    _timer = _timers.once(
      delay.isNegative ? Duration.zero : delay,
      () => unawaited(_dispatchDue()),
    );
  }

  Future<void> _dispatchDue() async {
    if (_disposed) return;
    final now = _clock.now();
    final due = _entries.values.where((e) => !e.fireTime.isAfter(now)).toList()
      ..sort((a, b) => a.fireTime.compareTo(b.fireTime));
    for (final entry in due) {
      try {
        final handler = switch (entry.action) {
          ScheduleAction.startQueue => onStartQueue,
          ScheduleAction.startTask => onStartTask,
          ScheduleAction.pauseAll => onPauseAll,
          ScheduleAction.custom =>
            entry.callback == null ? onCustomAction : null,
        };
        if (entry.action == ScheduleAction.custom && entry.callback != null) {
          await entry.callback!();
        } else if (handler != null) {
          await handler(entry);
        } else {
          _emit(entry, ScheduleEventKind.skipped, now);
          await _advance(entry, now);
          continue;
        }
        _emit(entry, ScheduleEventKind.fired, now);
      } catch (error) {
        _emit(entry, ScheduleEventKind.skipped, now, error: error);
      }
      await _advance(entry, now);
    }
    _arm();
  }

  Future<void> _advance(ScheduleEntry entry, DateTime after) async {
    final next = entry.nextAfter(after);
    if (next == null) {
      _entries.remove(entry.id);
    } else {
      _entries[entry.id] = entry.copyWith(fireTime: next);
    }
    await _persist();
  }

  void _emit(
    ScheduleEntry e,
    ScheduleEventKind kind,
    DateTime at, {
    Object? error,
  }) {
    if (!_events.isClosed)
      _events.add(ScheduleEvent(entry: e, kind: kind, at: at, error: error));
  }

  Future<void> _persist() async {
    await onPersist?.call(entries);
    final s = store;
    if (s != null) {
      final old = await s.load();
      await s.save(
        QueueSchedulerState(queues: old?.queues ?? const [], entries: entries),
      );
    }
  }

  void _ensureAlive() {
    if (_disposed) throw StateError('TaskScheduler is disposed');
  }

  Future<void> dispose() async {
    _disposed = true;
    _timer?.cancel();
    await _events.close();
  }
}
