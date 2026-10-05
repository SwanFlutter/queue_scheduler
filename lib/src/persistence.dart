import 'package:download_persistence/download_persistence.dart' as persistence;

import 'download_queue.dart';
import 'scheduler.dart';

/// Repository for queue and schedule metadata. Tasks themselves remain owned
/// by download_engine.
abstract interface class QueueSchedulerStore {
  Future<QueueSchedulerState?> load();
  Future<void> save(QueueSchedulerState state);
}

final class QueueSchedulerState {
  const QueueSchedulerState({this.queues = const [], this.entries = const []});
  final List<DownloadQueue> queues;
  final List<ScheduleEntry> entries;
}

/// Stores one tagged metadata record through download_persistence's TaskStore.
/// Give it a dedicated store/file so the metadata record is not restored as a
/// download task by a download engine.
final class TaskStoreQueueSchedulerStore implements QueueSchedulerStore {
  TaskStoreQueueSchedulerStore(this.store);
  static const recordId = '__queue_scheduler_state__';
  final persistence.TaskStore store;

  @override
  Future<QueueSchedulerState?> load() async {
    final records = await store.loadAll();
    for (final r in records) {
      if (r['id'] != recordId) continue;
      final raw = r['queueSchedulerData'];
      if (raw is! Map) return null;
      final data = raw.cast<String, dynamic>();
      return QueueSchedulerState(
        queues: [
          for (final v in (data['queues'] as List? ?? const []))
            if (v is Map) DownloadQueue.fromJson(v.cast<String, dynamic>()),
        ],
        entries: [
          for (final v in (data['entries'] as List? ?? const []))
            if (v is Map) ScheduleEntry.fromJson(v.cast<String, dynamic>()),
        ],
      );
    }
    return null;
  }

  @override
  Future<void> save(QueueSchedulerState state) => store.upsert({
    'id': recordId,
    'url': 'queue-scheduler://metadata',
    'savePath': '.',
    'status': 'paused',
    'queueSchedulerData': {
      'queues': [for (final q in state.queues) q.toJson()],
      'entries': [for (final e in state.entries) e.toJson()],
    },
  });
}

/// JSON-backed default using the JSON TaskStore implementation from
/// download_persistence. Use a dedicated path, separate from download data.
final class JsonQueueSchedulerStore extends TaskStoreQueueSchedulerStore {
  JsonQueueSchedulerStore(String path, {persistence.StoreLogger? logger})
    : super(persistence.JsonTaskStore(path, logger: logger));
}
