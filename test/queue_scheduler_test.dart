import 'dart:async';

import 'package:download_engine/download_engine.dart';
import 'package:download_persistence/download_persistence.dart' as persistence;
import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:queue_scheduler/queue_scheduler.dart';

final class MutableClock implements Clock {
  MutableClock(this.value);
  DateTime value;
  @override
  DateTime now() => value;
  void advance(Duration d) => value = value.add(d);
}

final class FakeEngine implements QueueDownloadEngine {
  final _tasks = <String, FluxDownloadTask>{};
  final _taskEvents = StreamController<List<FluxDownloadTask>>.broadcast();
  final _events = StreamController<FluxDownloadEvent>.broadcast();
  int _max = 4;
  int sequence = 0;
  @override
  Stream<List<FluxDownloadTask>> get tasks => Stream.multi((c) {
    c.add(snapshot);
    final s = _taskEvents.stream.listen(c.add);
    c.onCancel = s.cancel;
  });
  @override
  Stream<FluxDownloadEvent> get events => _events.stream;
  @override
  List<FluxDownloadTask> get snapshot => List.unmodifiable(_tasks.values);
  @override
  FluxDownloadTask? taskById(String id) => _tasks[id];
  @override
  int get maxConcurrentDownloads => _max;
  @override
  set maxConcurrentDownloads(int value) => _max = value;
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
  }) {
    final id = 'task-${sequence++}';
    final t = FluxDownloadTask(
      id: id,
      url: url,
      fileName: id,
      savePath: '.',
      createdAt: DateTime.utc(2026),
      priority: priority,
      status: autoStart ? FluxDownloadStatus.queued : FluxDownloadStatus.paused,
    );
    _tasks[id] = t;
    _publish();
    return t;
  }

  @override
  void pause(String id) {
    final t = _tasks[id]!;
    _tasks[id] = t.copyWith(status: FluxDownloadStatus.paused);
    _publish();
  }

  @override
  void resume(String id) {
    final t = _tasks[id]!;
    final active = _tasks.values.where((t) => t.isActive).length;
    if (active >= _max) return;
    _tasks[id] = t.copyWith(status: FluxDownloadStatus.downloading);
    _events.add(
      FluxDownloadEvent(kind: FluxDownloadEventKind.started, task: _tasks[id]!),
    );
    _publish();
  }

  @override
  void setSpeedLimitBps(String id, int bps) {}
  void _publish() => _taskEvents.add(snapshot);
  Future<void> close() async {
    await _taskEvents.close();
    await _events.close();
  }
}

final class MemoryTaskStore extends persistence.TaskStore {
  final rows = <String, Map<String, dynamic>>{};
  @override
  Future<List<Map<String, dynamic>>> loadAll() async =>
      rows.values.map((row) => Map<String, dynamic>.from(row)).toList();
  @override
  Future<void> upsert(Map<String, dynamic> task) async {
    rows[task['id'] as String] = Map<String, dynamic>.from(task);
  }

  @override
  Future<void> remove(String id) async {
    rows.remove(id);
  }

  @override
  Future<void> clear() async => rows.clear();
}

void main() {
  test('queue concurrency of two is respected for ten tasks', () {
    fakeAsync((async) {
      final clock = MutableClock(DateTime(2026, 1, 1));
      final engine = FakeEngine();
      final manager = QueueManager.withEngine(
        engine: engine,
        maxActiveDownloads: 4,
        clock: clock,
        timers: const DartTimerFactory(),
      );
      manager.addQueue(
        DownloadQueue(
          id: 'q',
          name: 'bulk',
          status: QueueStatus.running,
          concurrency: 2,
          itemIds: List.generate(10, (i) => 'task-$i'),
        ),
      );
      for (var i = 0; i < 10; i++) {
        engine.add(url: 'https://example.test/$i', autoStart: false);
      }
      async.flushMicrotasks();
      manager.restore();
      async.flushMicrotasks();
      expect(engine.snapshot.where((t) => t.isActive).length, 2);
      async.elapse(const Duration(seconds: 1));
      expect(
        engine.snapshot.where((t) => t.isActive).length,
        lessThanOrEqualTo(2),
      );
      manager.dispose();
      engine.close();
    });
  });

  test('daily entry advances to the next occurrence after firing', () async {
    final clock = MutableClock(DateTime(2026, 1, 1, 8));
    final entry = ScheduleEntry(
      id: 'daily',
      label: 'start',
      fireTime: DateTime(2026, 1, 1, 8, 1),
      recurrence: Recurrence.daily,
      action: ScheduleAction.startQueue,
      assignedQueueId: 'q',
    );
    final fired = <ScheduleEventKind>[];
    late TaskScheduler scheduler;
    fakeAsync((async) {
      scheduler = TaskScheduler(
        clock: clock,
        onStartQueue: (_) {
          fired.add(ScheduleEventKind.fired);
        },
        onPersist: (_) async {},
      );
      scheduler.add(entry);
      async.flushMicrotasks();
      clock.advance(const Duration(minutes: 1));
      async.elapse(const Duration(minutes: 1));
      async.flushMicrotasks();
    });
    expect(fired, [ScheduleEventKind.fired]);
    expect(scheduler.entries.single.fireTime, DateTime(2026, 1, 2, 8, 1));
    await scheduler.dispose();
  });

  test(
    'queue membership and pending schedule restore from TaskStore',
    () async {
      final repository = TaskStoreQueueSchedulerStore(MemoryTaskStore());
      final queue = DownloadQueue(
        id: 'saved',
        name: 'restored queue',
        status: QueueStatus.paused,
        concurrency: 2,
        itemIds: const ['a', 'b', 'c'],
      );
      final entry = ScheduleEntry(
        id: 'pending',
        label: 'resume later',
        fireTime: DateTime(2027, 4, 1, 9),
        recurrence: Recurrence.daily,
        action: ScheduleAction.startQueue,
        assignedQueueId: queue.id,
      );
      await repository.save(
        QueueSchedulerState(queues: [queue], entries: [entry]),
      );
      final restored = await repository.load();
      expect(restored!.queues.single.itemIds, ['a', 'b', 'c']);
      expect(restored.entries.single.fireTime, entry.fireTime);
      expect(restored.entries.single.recurrence, Recurrence.daily);
    },
  );

  test(
    'expired recurring entry is reported missed and moved forward',
    () async {
      final clock = MutableClock(DateTime(2026, 1, 3, 9));
      final scheduler = TaskScheduler(clock: clock);
      final events = <ScheduleEvent>[];
      final sub = scheduler.events.listen(events.add);
      await scheduler.restore([
        ScheduleEntry(
          id: 'missed-daily',
          label: 'missed while closed',
          fireTime: DateTime(2026, 1, 2, 9),
          recurrence: Recurrence.daily,
        ),
      ]);
      await Future<void>.delayed(Duration.zero);
      expect(events.single.kind, ScheduleEventKind.missed);
      expect(scheduler.entries.single.fireTime, DateTime(2026, 1, 4, 9));
      await sub.cancel();
      await scheduler.dispose();
    },
  );
}
