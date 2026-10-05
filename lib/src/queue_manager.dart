import 'dart:async';

import 'package:download_engine/download_engine.dart';

import 'clock.dart';
import 'download_queue.dart';
import 'persistence.dart';

/// Small engine surface used by the manager and by deterministic fakes.
abstract interface class QueueDownloadEngine {
  Stream<List<FluxDownloadTask>> get tasks;
  Stream<FluxDownloadEvent> get events;
  List<FluxDownloadTask> get snapshot;
  FluxDownloadTask? taskById(String id);
  int get maxConcurrentDownloads;
  set maxConcurrentDownloads(int value);
  FluxDownloadTask add({
    required String url,
    String? fileName,
    String? savePath,
    int threads = 8,
    String category = '',
    FluxPriority priority = FluxPriority.normal,
    int speedLimitBps = 0,
    FluxChecksum? checksum,
    Map<String, String> headers = const {},
    bool autoStart = true,
  });
  void pause(String id);
  void resume(String id);
  void setSpeedLimitBps(String id, int bps);
}

/// Adapter for the concrete download_engine implementation.
final class DownloadEngineAdapter implements QueueDownloadEngine {
  DownloadEngineAdapter(this.engine);
  final FluxDownloadEngine engine;
  @override
  Stream<List<FluxDownloadTask>> get tasks => engine.tasks;
  @override
  Stream<FluxDownloadEvent> get events => engine.events;
  @override
  List<FluxDownloadTask> get snapshot => engine.snapshot;
  @override
  FluxDownloadTask? taskById(String id) => engine.taskById(id);
  @override
  int get maxConcurrentDownloads => engine.maxConcurrentDownloads;
  @override
  set maxConcurrentDownloads(int value) =>
      engine.maxConcurrentDownloads = value;
  @override
  FluxDownloadTask add({
    required String url,
    String? fileName,
    String? savePath,
    int threads = 8,
    String category = '',
    FluxPriority priority = FluxPriority.normal,
    int speedLimitBps = 0,
    FluxChecksum? checksum,
    Map<String, String> headers = const {},
    bool autoStart = true,
  }) => engine.add(
    url: url,
    fileName: fileName,
    savePath: savePath,
    threads: threads,
    category: category,
    priority: priority,
    speedLimitBps: speedLimitBps,
    checksum: checksum,
    headers: headers,
    autoStart: autoStart,
  );
  @override
  void pause(String id) => engine.pause(id);
  @override
  void resume(String id) => engine.resume(id);
  @override
  void setSpeedLimitBps(String id, int bps) => engine.setSpeedLimitBps(id, bps);
}

/// Coordinates task starts while leaving transfer and retry mechanics to the
/// download engine. Tasks should be added with [createTask] or `autoStart:false`
/// so they cannot start before queue rules are applied.
final class QueueManager {
  QueueManager({
    required FluxDownloadEngine engine,
    int maxActiveDownloads = 4,
    Clock clock = const SystemClock(),
    TimerFactory timers = const DartTimerFactory(),
    this.store,
    this.onPersist,
  }) : engine = DownloadEngineAdapter(engine),
       _maxActiveDownloads = maxActiveDownloads,
       _clock = clock,
       _timers = timers {
    _initialize();
  }

  QueueManager.withEngine({
    required QueueDownloadEngine engine,
    int maxActiveDownloads = 4,
    Clock clock = const SystemClock(),
    TimerFactory timers = const DartTimerFactory(),
    this.store,
    this.onPersist,
  }) : engine = engine,
       _maxActiveDownloads = maxActiveDownloads,
       _clock = clock,
       _timers = timers {
    _initialize();
  }

  void _initialize() {
    if (maxActiveDownloads < 1) throw ArgumentError.value(maxActiveDownloads);
    engine.maxConcurrentDownloads = maxActiveDownloads;
    _taskSub = engine.tasks.listen((_) {
      _publishProgress();
      _pump();
      _checkDrained();
    });
    _eventSub = engine.events.listen((_) {
      _publishProgress();
      _pump();
      _checkDrained();
    });
    _scheduleTimer = _timers.periodic(
      const Duration(milliseconds: 250),
      (_) => _pump(),
    );
  }

  final QueueDownloadEngine engine;
  int _maxActiveDownloads;
  int get maxActiveDownloads => _maxActiveDownloads;
  set maxActiveDownloads(int value) {
    if (value < 1) throw ArgumentError.value(value, 'maxActiveDownloads');
    _maxActiveDownloads = value;
    engine.maxConcurrentDownloads = value;
    _pump();
  }

  final Clock _clock;
  final TimerFactory _timers;
  final QueueSchedulerStore? store;
  final Future<void> Function(List<DownloadQueue>)? onPersist;
  final Map<String, DownloadQueue> _queues = {};
  final _progress = StreamController<QueueProgress>.broadcast();
  final _drained = StreamController<void>.broadcast();
  late final StreamSubscription<List<FluxDownloadTask>> _taskSub;
  late final StreamSubscription<FluxDownloadEvent> _eventSub;
  late final Timer _scheduleTimer;
  bool _disposed = false;
  bool _hadWork = false;
  bool _drainedEmitted = false;

  List<DownloadQueue> get queues => List.unmodifiable(_queues.values);
  Stream<QueueProgress> get progress => _progress.stream;
  Stream<void> get onAllQueuesDrained => _drained.stream;
  Stream<QueueProgress> watchQueue(String id) =>
      progress.where((p) => p.queueId == id);

  Future<void> restore() async {
    final saved = await store?.load();
    if (saved != null) {
      _queues
        ..clear()
        ..addEntries(saved.queues.map((q) => MapEntry(q.id, q)));
      for (final q in _queues.values.toList()) {
        _hadWork = _hadWork || q.itemIds.isNotEmpty;
        if (q.status == QueueStatus.running) {
          _queues[q.id] = q.copyWith(status: QueueStatus.paused);
        }
      }
      await _persist();
      _pump();
    }
  }

  Future<void> addQueue(DownloadQueue queue) async {
    _ensureAlive();
    if (_queues.containsKey(queue.id))
      throw ArgumentError('duplicate queue ${queue.id}');
    _queues[queue.id] = queue;
    _hadWork = _hadWork || queue.itemIds.isNotEmpty;
    if (queue.itemIds.isNotEmpty) _drainedEmitted = false;
    if (queue.startsAt != null) {
      _queues[queue.id] = queue.copyWith(status: QueueStatus.scheduled);
    }
    await _persist();
    _pump();
  }

  Future<FluxDownloadTask> createTask(
    String queueId, {
    required String url,
    String? fileName,
    String? savePath,
    int threads = 8,
    String category = '',
    FluxPriority priority = FluxPriority.normal,
    int speedLimitBps = 0,
    FluxChecksum? checksum,
    Map<String, String> headers = const {},
  }) async {
    _requireQueue(queueId);
    final task = engine.add(
      url: url,
      fileName: fileName,
      savePath: savePath,
      threads: threads,
      category: category,
      priority: priority,
      speedLimitBps: speedLimitBps,
      checksum: checksum,
      headers: headers,
      autoStart: false,
    );
    await addItem(queueId, task.id);
    return task;
  }

  Future<void> addItem(String queueId, String taskId, {int? index}) async {
    final q = _requireQueue(queueId);
    if (engine.taskById(taskId) == null)
      throw ArgumentError.value(taskId, 'taskId', 'unknown task');
    if (q.itemIds.contains(taskId)) return;
    final ids = [...q.itemIds];
    ids.insert((index ?? ids.length).clamp(0, ids.length).toInt(), taskId);
    _queues[queueId] = q.copyWith(itemIds: ids);
    _hadWork = true;
    _drainedEmitted = false;
    await _persist();
    _pump();
  }

  Future<void> removeItem(String queueId, String taskId) async {
    final q = _requireQueue(queueId);
    if (!q.itemIds.contains(taskId)) return;
    final task = engine.taskById(taskId);
    if (task != null && !task.status.isTerminal) engine.pause(taskId);
    _queues[queueId] = q.copyWith(
      itemIds: q.itemIds.where((id) => id != taskId).toList(),
    );
    await _persist();
    _pump();
    _checkDrained();
  }

  Future<void> moveToTop(String queueId, String taskId) =>
      move(queueId, taskId, 0);
  Future<void> move(String queueId, String taskId, int index) async {
    final q = _requireQueue(queueId);
    if (!q.itemIds.contains(taskId))
      throw ArgumentError.value(taskId, 'taskId');
    final ids = [...q.itemIds]..remove(taskId);
    ids.insert(index.clamp(0, ids.length).toInt(), taskId);
    _queues[queueId] = q.copyWith(itemIds: ids);
    await _persist();
    _pump();
  }

  Future<void> pauseQueue(String queueId) async {
    final q = _requireQueue(queueId);
    _queues[queueId] = q.copyWith(status: QueueStatus.paused);
    for (final id in q.itemIds) {
      final task = engine.taskById(id);
      if (task != null &&
          !task.status.isTerminal &&
          task.status != FluxDownloadStatus.paused) {
        engine.pause(id);
      }
    }
    await _persist();
    _publishProgress();
  }

  Future<void> resumeQueue(String queueId) async {
    final q = _requireQueue(queueId);
    _queues[queueId] = q.copyWith(status: QueueStatus.running, startsAt: null);
    await _persist();
    _pump();
  }

  Future<void> updateRules(
    String queueId, {
    int? concurrency,
    int? bandwidthLimitBps,
  }) async {
    final q = _requireQueue(queueId);
    _queues[queueId] = q.copyWith(
      concurrency: concurrency,
      bandwidthLimitBps: bandwidthLimitBps,
    );
    await _persist();
    _pump();
  }

  Future<void> removeQueue(String queueId) async {
    final q = _requireQueue(queueId);
    for (final id in q.itemIds) {
      final t = engine.taskById(id);
      if (t != null && !t.status.isTerminal) engine.pause(id);
    }
    _queues.remove(queueId);
    await _persist();
    _checkDrained();
  }

  void _pump() {
    if (_disposed) return;
    final now = _clock.now();
    var queueStateChanged = false;
    for (final q in _queues.values.toList()) {
      if (q.status == QueueStatus.scheduled &&
          q.startsAt != null &&
          !q.startsAt!.isAfter(now)) {
        _queues[q.id] = q.copyWith(status: QueueStatus.running, startsAt: null);
        queueStateChanged = true;
      } else if (q.status == QueueStatus.running && q.itemIds.isNotEmpty) {
        final allFinished = q.itemIds
            .map(engine.taskById)
            .whereType<FluxDownloadTask>()
            .every((t) => t.status.isTerminal);
        if (allFinished) {
          _queues[q.id] = q.copyWith(status: QueueStatus.idle);
          queueStateChanged = true;
        }
      }
    }
    var active = engine.snapshot.where((t) => t.isActive).length;
    final available =
        _queues.values
            .where((q) => q.status == QueueStatus.running)
            .expand((q) => q.itemIds.map((id) => (q: q, id: id)))
            .where(
              (x) =>
                  engine.taskById(x.id)?.status == FluxDownloadStatus.paused ||
                  engine.taskById(x.id)?.status == FluxDownloadStatus.queued,
            )
            .toList()
          ..sort((a, b) {
            final at = engine.taskById(a.id)!;
            final bt = engine.taskById(b.id)!;
            final priority = bt.priority.index.compareTo(at.priority.index);
            if (priority != 0) return priority;
            final aq = a.q.itemIds.indexOf(a.id);
            final bq = b.q.itemIds.indexOf(b.id);
            return aq.compareTo(bq);
          });
    final perQueue = <String, int>{};
    for (final q in _queues.values) {
      perQueue[q.id] = q.itemIds
          .map(engine.taskById)
          .whereType<FluxDownloadTask>()
          .where((t) => t.isActive)
          .length;
    }
    for (final candidate in available) {
      if (active >= maxActiveDownloads) break;
      if ((perQueue[candidate.q.id] ?? 0) >= candidate.q.concurrency) continue;
      final task = engine.taskById(candidate.id);
      if (task == null) continue;
      if (candidate.q.bandwidthLimitBps > 0) {
        engine.setSpeedLimitBps(
          candidate.id,
          _effectiveLimit(candidate.q, task, perQueue[candidate.q.id]! + 1),
        );
      }
      engine.resume(candidate.id);
      active++;
      perQueue[candidate.q.id] = (perQueue[candidate.q.id] ?? 0) + 1;
    }
    for (final q in _queues.values) {
      final count = perQueue[q.id] ?? 0;
      if (q.bandwidthLimitBps > 0 && count > 0) {
        for (final id in q.itemIds) {
          final t = engine.taskById(id);
          if (t?.isActive == true) {
            final limit = _effectiveLimit(q, t!, count);
            if (t.speedLimitBps != limit) engine.setSpeedLimitBps(id, limit);
          }
        }
      }
    }
    if (queueStateChanged) unawaited(_persist());
    _publishProgress();
    _checkDrained();
  }

  int _effectiveLimit(DownloadQueue q, FluxDownloadTask task, int activeCount) {
    final share = (q.bandwidthLimitBps / activeCount)
        .floor()
        .clamp(1, q.bandwidthLimitBps)
        .toInt();
    return task.speedLimitBps == 0 || share < task.speedLimitBps
        ? share
        : task.speedLimitBps;
  }

  void _publishProgress() {
    for (final q in _queues.values) {
      final tasks = q.itemIds
          .map(engine.taskById)
          .whereType<FluxDownloadTask>()
          .toList();
      final totalWeight = tasks.fold<double>(
        0,
        (s, t) => s + (t.sizeBytes > 0 ? t.sizeBytes : 1),
      );
      final doneWeight = tasks.fold<double>(
        0,
        (s, t) =>
            s +
            (t.sizeBytes > 0
                ? t.sizeBytes * t.progressPercent / 100
                : t.progressPercent / 100),
      );
      final speed = tasks.fold<double>(0, (s, t) => s + t.speedBps);
      final remaining = tasks.fold<double>(
        0,
        (s, t) =>
            s +
            (t.sizeBytes > 0
                ? t.sizeBytes * (100 - t.progressPercent) / 100
                : 0),
      );
      final completed = tasks
          .where((t) => t.status == FluxDownloadStatus.completed)
          .length;
      _progress.add(
        QueueProgress(
          queueId: q.id,
          percent: totalWeight == 0
              ? 0
              : (doneWeight / totalWeight * 100).clamp(0, 100).toDouble(),
          eta: speed > 0 && tasks.every((t) => t.sizeBytes > 0)
              ? Duration(seconds: (remaining / speed).ceil())
              : null,
          speedBps: speed,
          completed: completed,
          total: tasks.length,
        ),
      );
    }
  }

  void _checkDrained() {
    final unfinished = _queues.values
        .expand((q) => q.itemIds)
        .map(engine.taskById)
        .whereType<FluxDownloadTask>()
        .any((t) => !t.status.isTerminal);
    if (_hadWork && !unfinished && !_drainedEmitted) {
      _drainedEmitted = true;
      if (!_drained.isClosed) _drained.add(null);
    }
  }

  Future<void> _persist() async {
    await onPersist?.call(queues);
    final s = store;
    if (s != null) {
      final old = await s.load();
      await s.save(
        QueueSchedulerState(queues: queues, entries: old?.entries ?? const []),
      );
    }
  }

  DownloadQueue _requireQueue(String id) =>
      _queues[id] ??
      (throw ArgumentError.value(id, 'queueId', 'unknown queue'));
  void _ensureAlive() {
    if (_disposed) throw StateError('QueueManager is disposed');
  }

  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _scheduleTimer.cancel();
    await _taskSub.cancel();
    await _eventSub.cancel();
    await _progress.close();
    await _drained.close();
  }
}
