import 'dart:async';
import 'package:download_engine/download_engine.dart';
import 'package:download_persistence/download_persistence.dart' as persistence;
import 'package:queue_scheduler/queue_scheduler.dart';

/// In-memory TaskStore for pure Dart example.
final class DemoMemoryTaskStore extends persistence.TaskStore {
  final Map<String, Map<String, dynamic>> _data = {};

  @override
  Future<List<Map<String, dynamic>>> loadAll() async {
    return _data.values.map((e) => Map<String, dynamic>.from(e)).toList();
  }

  @override
  Future<void> upsert(Map<String, dynamic> task) async {
    _data[task['id'] as String] = Map<String, dynamic>.from(task);
  }

  @override
  Future<void> remove(String id) async {
    _data.remove(id);
  }

  @override
  Future<void> clear() async {
    _data.clear();
  }
}

Future<void> main() async {
  print('=== Queue Scheduler Pure Dart Example ===\n');

  final persistenceStore = DemoMemoryTaskStore();

  // 1. Initialize Download Engine and Storage
  final engine = FluxDownloadEngine(
    maxConcurrentDownloads: 4,
    store: InMemoryTaskStore(),
  );

  final store = TaskStoreQueueSchedulerStore(persistenceStore);

  // 2. Initialize QueueManager
  final queueManager = QueueManager(
    engine: engine,
    maxActiveDownloads: 4,
    store: store,
  );

  // 3. Initialize TaskScheduler
  final scheduler = TaskScheduler(
    store: store,
    onStartQueue: (entry) async {
      print('⏰ [Scheduler Action] Starting queue: ${entry.assignedQueueId}');
      if (entry.assignedQueueId != null) {
        await queueManager.resumeQueue(entry.assignedQueueId!);
      }
    },
    onPauseAll: (entry) async {
      print('⏰ [Scheduler Action] Pausing all queues');
      for (final q in queueManager.queues) {
        await queueManager.pauseQueue(q.id);
      }
    },
    onCustomAction: (entry) async {
      print('⏰ [Scheduler Action] Custom action: ${entry.actionName}');
    },
  );

  // Listen to queue aggregate progress
  final progressSub = queueManager.progress.listen((p) {
    print(
      '📊 [Progress - ${p.queueId}] ${p.percent.toStringAsFixed(1)}% | '
      'Speed: ${(p.speedBps / 1024).toStringAsFixed(1)} KB/s | '
      'Tasks: ${p.completed}/${p.total}',
    );
  });

  // Listen to completion hook (auto-shutdown trigger)
  final drainedSub = queueManager.onAllQueuesDrained.listen((_) {
    print('\n🎉 [onAllQueuesDrained] All queues completed all downloads!');
  });

  // Listen to scheduler events
  final schedulerSub = scheduler.events.listen((event) {
    print('🔔 [Schedule Event] ${event.entry.label} -> ${event.kind.name} at ${event.at}');
  });

  // 4. Create Queues
  print('Creating queues...');
  final highPriorityQueue = DownloadQueue(
    id: 'queue-fast',
    name: 'High Priority Queue',
    status: QueueStatus.running,
    concurrency: 2, // max 2 parallel downloads in this queue
    bandwidthLimitBps: 10 * 1024 * 1024, // 10 MB/s
  );

  final nightQueue = DownloadQueue(
    id: 'queue-night',
    name: 'Night Batch Queue',
    status: QueueStatus.paused,
    concurrency: 1,
    bandwidthLimitBps: 2 * 1024 * 1024, // 2 MB/s
  );

  await queueManager.addQueue(highPriorityQueue);
  await queueManager.addQueue(nightQueue);

  // 5. Add Tasks
  print('Adding tasks...');
  final task1 = await queueManager.createTask(
    highPriorityQueue.id,
    url: 'https://example.com/file1.zip',
    fileName: 'file1.zip',
    priority: FluxPriority.high,
  );

  final task2 = await queueManager.createTask(
    highPriorityQueue.id,
    url: 'https://example.com/file2.zip',
    fileName: 'file2.zip',
    priority: FluxPriority.normal,
  );

  final task3 = await queueManager.createTask(
    highPriorityQueue.id,
    url: 'https://example.com/file3.zip',
    fileName: 'file3.zip',
    priority: FluxPriority.low,
  );

  print('Added tasks: ${task1.id}, ${task2.id}, ${task3.id}');
  print('Active downloads in engine: ${engine.snapshot.where((t) => t.isActive).length}');

  // 6. Schedule a timer (e.g. daily trigger or 1-second delayed test)
  print('Scheduling night queue start...');
  await scheduler.add(
    ScheduleEntry(
      id: 'sched-night',
      label: 'Start Night Batch',
      fireTime: DateTime.now().add(const Duration(seconds: 1)),
      recurrence: Recurrence.daily,
      action: ScheduleAction.startQueue,
      assignedQueueId: nightQueue.id,
    ),
  );

  // Wait a bit for simulated execution
  await Future.delayed(const Duration(seconds: 2));

  // Clean up
  await progressSub.cancel();
  await drainedSub.cancel();
  await schedulerSub.cancel();
  await queueManager.dispose();
  await scheduler.dispose();
  await engine.dispose();

  print('\n=== Example Finished ===');
}
