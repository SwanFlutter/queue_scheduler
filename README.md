queue_scheduler
A high-performance, pure Dart queue execution and scheduling layer designed for download managers. It coordinates download tasks from named queues with concurrency and bandwidth rules, and triggers automated actions at scheduled times.

Built to seamlessly wrap download_engine and integrate with download_persistence.

Features
Pure Dart & Lightweight: No Flutter or GetX dependency in the core package — clean, testable, and suitable for Windows, macOS, Linux, and Android desktop/mobile integrations.
Named Download Queues (DownloadQueue):
Independent queue states: idle, running, paused, scheduled.
Configurable per-queue concurrency (max simultaneous tasks per queue).
Dynamic bandwidth management: Queue bandwidth limits are evenly divided across active tasks in real-time.
Auto-start scheduling via startsAt.
Advanced Queue Coordination (QueueManager):
Enforces both per-queue concurrency and global active download caps.
Priority-Aware: High-priority tasks automatically jump the queue.
Queue item management: createTask, addItem, removeItem, moveToTop, move, pauseQueue, resumeQueue, updateRules.
Aggregate progress streams (QueueProgress): Real-time percentage, download speed (B/s), ETA duration, completed/total count.
Auto-Shutdown / Drain Hook: onAllQueuesDrained stream fires when all queues have completed their tasks.
Flexible Task Scheduler (TaskScheduler):
Accurate timer-based dispatcher with recurring patterns: once, daily, weekly, weekdays, customWeekdays.
Supports schedule time windows (ScheduleWindow with start/end minutes).
Out-of-the-box actions (startQueue, startTask, pauseAll, custom).
Missed Run Detection: Automatically detects and reports schedule entries that expired while the application was closed.
Real-time ScheduleEvent stream (fired, skipped, missed).
Crash-Safe Persistence:
Serializes queue state and schedule entries via download_persistence's TaskStore (JsonQueueSchedulerStore).
Deterministic Testing:
Injectable Clock and TimerFactory abstractions for instant, flake-free unit tests with fake_async.
Installation
Add queue_scheduler to your pubspec.yaml:


dependencies:
  queue_scheduler:
    path: ../queue_scheduler
  download_engine:
    path: ../download_engine
  download_persistence:
    path: ../download_persistence
Quick Start
1. Initialize Engine, Store, and QueueManager

import 'package:download_engine/download_engine.dart';
import 'package:download_persistence/download_persistence.dart' as persistence;
import 'package:queue_scheduler/queue_scheduler.dart';

void main() async {
  // 1. Storage setup
  final taskStore = persistence.JsonTaskStore('path/to/queue_metadata.json');
  final schedulerStore = TaskStoreQueueSchedulerStore(taskStore);

  // 2. Download Engine
  final engine = FluxDownloadEngine(
    maxConcurrentDownloads: 4,
  );

  // 3. Queue Manager
  final queueManager = QueueManager(
    engine: engine,
    maxActiveDownloads: 4,
    store: schedulerStore,
  );

  // 4. Task Scheduler
  final scheduler = TaskScheduler(
    store: schedulerStore,
    onStartQueue: (entry) async {
      if (entry.assignedQueueId != null) {
        await queueManager.resumeQueue(entry.assignedQueueId!);
      }
    },
    onPauseAll: (entry) async {
      for (final q in queueManager.queues) {
        await queueManager.pauseQueue(q.id);
      }
    },
    onCustomAction: (entry) async {
      // Connect to host OS actions (e.g., shutdown, sleep)
      if (entry.actionName == 'shutdownSystem') {
        // Trigger OS shutdown
      }
    },
  );

  // Restore persisted state on startup
  await queueManager.restore();
  await scheduler.restore();
}
2. Creating and Managing Queues

// Define a queue with 2 max parallel downloads and a 5 MB/s bandwidth cap
final videoQueue = DownloadQueue(
  id: 'queue-videos',
  name: 'Video Downloads',
  status: QueueStatus.running,
  concurrency: 2,
  bandwidthLimitBps: 5 * 1024 * 1024, // 5 MB/s
);

await queueManager.addQueue(videoQueue);

// Add download tasks with priorities
final highPriorityTask = await queueManager.createTask(
  videoQueue.id,
  url: 'https://example.com/movie_part1.mp4',
  fileName: 'movie_part1.mp4',
  priority: FluxPriority.high, // Jumps the line within the queue
);

final normalTask = await queueManager.createTask(
  videoQueue.id,
  url: 'https://example.com/movie_part2.mp4',
  fileName: 'movie_part2.mp4',
  priority: FluxPriority.normal,
);

// Reordering tasks
await queueManager.moveToTop(videoQueue.id, normalTask.id);
3. Monitoring Live Progress & Completion Hook

// Stream aggregate progress for all queues or a specific queue
queueManager.progress.listen((progress) {
  print('Queue: ${progress.queueId}');
  print('Progress: ${progress.percent.toStringAsFixed(1)}%');
  print('Speed: ${(progress.speedBps / 1024).toStringAsFixed(1)} KB/s');
  print('Tasks Completed: ${progress.completed}/${progress.total}');
  if (progress.eta != null) {
    print('ETA: ${progress.eta!.inSeconds} seconds');
  }
});

// Automatic shutdown hook: fires when all queues have finished all items
queueManager.onAllQueuesDrained.listen((_) {
  print('All queues are drained! Triggering system power-off...');
});
4. Scheduling Queues and Recurring Tasks

// Schedule a queue to automatically start daily at 02:00 AM
final nightSchedule = ScheduleEntry(
  id: 'nightly-downloads',
  label: 'Start Nightly Queue',
  fireTime: DateTime(2026, 10, 6, 2, 0),
  recurrence: Recurrence.daily,
  action: ScheduleAction.startQueue,
  assignedQueueId: 'queue-videos',
);

await scheduler.add(nightSchedule);

// Listen to scheduler events
scheduler.events.listen((event) {
  switch (event.kind) {
    case ScheduleEventKind.fired:
      print('Scheduler fired: ${event.entry.label}');
      break;
    case ScheduleEventKind.missed:
      print('Scheduler missed entry while app was closed: ${event.entry.label}');
      break;
    case ScheduleEventKind.skipped:
      print('Scheduler skipped entry: ${event.entry.label}');
      break;
  }
});
Architecture & Concepts
Bandwidth Division
When a DownloadQueue has a bandwidthLimitBps set, QueueManager calculates: 
Task Limit
=
⌊
Queue Bandwidth Limit
Active Tasks in Queue
⌋
Task Limit=⌊ 
Active Tasks in Queue
Queue Bandwidth Limit
​
 ⌋ This ensures fair bandwidth sharing dynamically as downloads start and complete.

Missed Entry Handling
If the application is closed when a schedule entry was supposed to fire, calling scheduler.restore() on startup:

Detects that entry.fireTime < now.
Emits a ScheduleEventKind.missed event on scheduler.events.
Advances recurring entries to their next valid execution timestamp.
Testing
Run unit tests deterministically using flutter test or dart test:


flutter test
All scheduler timing logic and queue concurrency boundaries are tested using FakeEngine, MutableClock, and fake_async.

Example App
Check the example/ directory for:

Flutter UI App (example/lib/main.dart): An interactive dashboard showing live queue progress, queue rule configuration, and scheduler management.
Pure Dart CLI (example/pure_dart_example.dart): A console script demonstrating core engine and queue scheduling without Flutter dependencies.



path: G:\Android\Pakege\queue_scheduler

https://github.com/SwanFlutter/queue_scheduler