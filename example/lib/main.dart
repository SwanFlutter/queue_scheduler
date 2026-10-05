import 'dart:async';
import 'package:flutter/material.dart';
import 'package:download_engine/download_engine.dart';
import 'package:download_persistence/download_persistence.dart' as persistence;
import 'package:queue_scheduler/queue_scheduler.dart';

void main() {
  runApp(const QueueSchedulerExampleApp());
}

/// In-memory TaskStore for demonstration purposes.
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

class QueueSchedulerExampleApp extends StatelessWidget {
  const QueueSchedulerExampleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Queue & Scheduler Example',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(
          seedColor: Colors.indigo,
          brightness: Brightness.light,
        ),
        useMaterial3: true,
      ),
      home: const QueueSchedulerHomePage(),
    );
  }
}

class QueueSchedulerHomePage extends StatefulWidget {
  const QueueSchedulerHomePage({super.key});

  @override
  State<QueueSchedulerHomePage> createState() => _QueueSchedulerHomePageState();
}

class _QueueSchedulerHomePageState extends State<QueueSchedulerHomePage>
    with SingleTickerProviderStateMixin {
  late final TabController _tabController;
  late final FluxDownloadEngine _engine;
  late final QueueSchedulerStore _store;
  late final QueueManager _queueManager;
  late final TaskScheduler _scheduler;

  StreamSubscription<QueueProgress>? _progressSub;
  StreamSubscription<void>? _drainedSub;
  StreamSubscription<ScheduleEvent>? _schedulerEventsSub;

  final Map<String, QueueProgress> _progressMap = {};
  final List<String> _logs = [];
  bool _isInitialized = false;

  @override
  void initState() {
    super.initState();
    _tabController = TabController(length: 3, vsync: this);
    _initEngineAndManager();
  }

  Future<void> _initEngineAndManager() async {
    final persistenceStore = DemoMemoryTaskStore();

    // 1. Initialize Download Engine
    _engine = FluxDownloadEngine(
      maxConcurrentDownloads: 4,
      store: InMemoryTaskStore(),
    );

    // 2. Initialize Persistence Store for Queue Scheduler
    _store = TaskStoreQueueSchedulerStore(persistenceStore);

    // 3. Initialize QueueManager
    _queueManager = QueueManager(
      engine: _engine,
      maxActiveDownloads: 4,
      store: _store,
    );

    // 4. Initialize TaskScheduler
    _scheduler = TaskScheduler(
      store: _store,
      onStartQueue: (entry) async {
        _log('⏰ Scheduler fired: startQueue -> ${entry.assignedQueueId}');
        if (entry.assignedQueueId != null) {
          await _queueManager.resumeQueue(entry.assignedQueueId!);
        }
      },
      onPauseAll: (entry) async {
        _log('⏰ Scheduler fired: pauseAll queues');
        for (final q in _queueManager.queues) {
          await _queueManager.pauseQueue(q.id);
        }
      },
      onCustomAction: (entry) async {
        _log('⏰ Scheduler fired custom action: ${entry.actionName}');
      },
    );

    // Listen to queue progress
    _progressSub = _queueManager.progress.listen((progress) {
      if (mounted) {
        setState(() {
          _progressMap[progress.queueId] = progress;
        });
      }
    });

    // Listen to onAllQueuesDrained (completion hook for auto-shutdown)
    _drainedSub = _queueManager.onAllQueuesDrained.listen((_) {
      _log('🎉 [onAllQueuesDrained] All queues completed all download tasks!');
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('All download queues have drained successfully!'),
            backgroundColor: Colors.green,
          ),
        );
      }
    });

    // Listen to scheduler events
    _schedulerEventsSub = _scheduler.events.listen((event) {
      _log(
        '🔔 Scheduler Event: ${event.entry.label} -> ${event.kind.name} at ${event.at}',
      );
    });

    // Create default queues
    await _setupInitialQueuesAndTasks();

    if (mounted) {
      setState(() {
        _isInitialized = true;
      });
    }
  }

  Future<void> _setupInitialQueuesAndTasks() async {
    // Create Queue 1: Media Downloads (concurrency 2, speed limit 5 MB/s)
    final mediaQueue = DownloadQueue(
      id: 'queue-media',
      name: 'Media Queue',
      status: QueueStatus.idle,
      concurrency: 2,
      bandwidthLimitBps: 5 * 1024 * 1024,
    );
    await _queueManager.addQueue(mediaQueue);

    // Add tasks to media queue
    await _queueManager.createTask(
      mediaQueue.id,
      url: 'https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_10mb.mp4',
      fileName: 'bunny_video_1.mp4',
      priority: FluxPriority.high,
    );
    await _queueManager.createTask(
      mediaQueue.id,
      url: 'https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_20mb.mp4',
      fileName: 'bunny_video_2.mp4',
      priority: FluxPriority.normal,
    );
    await _queueManager.createTask(
      mediaQueue.id,
      url: 'https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_50mb.mp4',
      fileName: 'bunny_video_3.mp4',
      priority: FluxPriority.low,
    );

    // Create Queue 2: Documents Queue
    final docsQueue = DownloadQueue(
      id: 'queue-docs',
      name: 'Documents & Archives',
      status: QueueStatus.idle,
      concurrency: 1,
      bandwidthLimitBps: 0, // unlimited
    );
    await _queueManager.addQueue(docsQueue);

    await _queueManager.createTask(
      docsQueue.id,
      url: 'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',
      fileName: 'document_spec.pdf',
      priority: FluxPriority.normal,
    );

    // Add a schedule entry: Starts media queue in 2 minutes
    final scheduleTime = DateTime.now().add(const Duration(minutes: 2));
    await _scheduler.add(
      ScheduleEntry(
        id: 'sched-media-start',
        label: 'Auto-start Media in 2 min',
        fireTime: scheduleTime,
        recurrence: Recurrence.once,
        action: ScheduleAction.startQueue,
        assignedQueueId: mediaQueue.id,
      ),
    );

    _log('Ready: Initialized 2 queues and 1 schedule timer.');
  }

  void _log(String message) {
    if (!mounted) return;
    setState(() {
      _logs.insert(0, '${DateTime.now().toIso8601String().substring(11, 19)}: $message');
      if (_logs.length > 50) _logs.removeLast();
    });
  }

  @override
  void dispose() {
    _tabController.dispose();
    _progressSub?.cancel();
    _drainedSub?.cancel();
    _schedulerEventsSub?.cancel();
    _queueManager.dispose();
    _scheduler.dispose();
    _engine.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (!_isInitialized) {
      return const Scaffold(
        body: Center(child: CircularProgressIndicator()),
      );
    }

    return Scaffold(
      appBar: AppBar(
        title: const Text('Download Queue & Scheduler'),
        bottom: TabBar(
          controller: _tabController,
          tabs: const [
            Tab(icon: Icon(Icons.queue), text: 'Queues'),
            Tab(icon: Icon(Icons.schedule), text: 'Schedules'),
            Tab(icon: Icon(Icons.list_alt), text: 'Activity Logs'),
          ],
        ),
      ),
      body: TabBarView(
        controller: _tabController,
        children: [
          _buildQueuesTab(),
          _buildSchedulesTab(),
          _buildLogsTab(),
        ],
      ),
      floatingActionButton: FloatingActionButton.extended(
        onPressed: _showAddNewQueueDialog,
        icon: const Icon(Icons.add),
        label: const Text('New Queue'),
      ),
    );
  }

  Widget _buildQueuesTab() {
    final queues = _queueManager.queues;
    return ListView.builder(
      padding: const EdgeInsets.all(12),
      itemCount: queues.length,
      itemBuilder: (context, index) {
        final q = queues[index];
        final progress = _progressMap[q.id];
        return Card(
          elevation: 2,
          margin: const EdgeInsets.only(bottom: 12),
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          q.name,
                          style: const TextStyle(
                            fontSize: 18,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                        Text(
                          'Concurrency: ${q.concurrency} | Limit: ${q.bandwidthLimitBps == 0 ? "Unlimited" : "${(q.bandwidthLimitBps / 1024 / 1024).toStringAsFixed(1)} MB/s"}',
                          style: TextStyle(
                            color: Colors.grey.shade600,
                            fontSize: 13,
                          ),
                        ),
                      ],
                    ),
                    _buildStatusChip(q.status),
                  ],
                ),
                const SizedBox(height: 12),
                if (progress != null) ...[
                  LinearProgressIndicator(
                    value: progress.percent / 100,
                    minHeight: 6,
                    borderRadius: BorderRadius.circular(4),
                  ),
                  const SizedBox(height: 8),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Text(
                        '${progress.percent.toStringAsFixed(1)}% (${progress.completed}/${progress.total} tasks)',
                        style: const TextStyle(fontSize: 12),
                      ),
                      Text(
                        'Speed: ${(progress.speedBps / 1024).toStringAsFixed(1)} KB/s',
                        style: const TextStyle(fontSize: 12),
                      ),
                      if (progress.eta != null)
                        Text(
                          'ETA: ${progress.eta!.inSeconds}s',
                          style: const TextStyle(fontSize: 12),
                        ),
                    ],
                  ),
                ],
                const SizedBox(height: 12),
                Wrap(
                  spacing: 8,
                  children: [
                    if (q.status == QueueStatus.running)
                      ElevatedButton.icon(
                        icon: const Icon(Icons.pause, size: 16),
                        label: const Text('Pause'),
                        onPressed: () => _queueManager.pauseQueue(q.id),
                      )
                    else
                      ElevatedButton.icon(
                        icon: const Icon(Icons.play_arrow, size: 16),
                        label: const Text('Resume / Start'),
                        onPressed: () => _queueManager.resumeQueue(q.id),
                      ),
                    OutlinedButton.icon(
                      icon: const Icon(Icons.add_link, size: 16),
                      label: const Text('Add Task'),
                      onPressed: () => _showAddTaskDialog(q.id),
                    ),
                    OutlinedButton.icon(
                      icon: const Icon(Icons.tune, size: 16),
                      label: const Text('Edit Rules'),
                      onPressed: () => _showEditRulesDialog(q),
                    ),
                  ],
                ),
                const Divider(height: 24),
                const Text(
                  'Tasks in Queue:',
                  style: TextStyle(fontWeight: FontWeight.w600, fontSize: 13),
                ),
                const SizedBox(height: 6),
                ...q.itemIds.map((taskId) {
                  final task = _engine.taskById(taskId);
                  if (task == null) return const SizedBox.shrink();
                  return ListTile(
                    dense: true,
                    contentPadding: EdgeInsets.zero,
                    title: Text(task.fileName.isNotEmpty ? task.fileName : task.url),
                    subtitle: Text(
                      'Status: ${task.status.name} | Priority: ${task.priority.name}',
                    ),
                    trailing: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        IconButton(
                          icon: const Icon(Icons.vertical_align_top, size: 18),
                          tooltip: 'Move to Top',
                          onPressed: () => _queueManager.moveToTop(q.id, taskId),
                        ),
                        IconButton(
                          icon: const Icon(Icons.delete_outline, size: 18, color: Colors.red),
                          tooltip: 'Remove',
                          onPressed: () => _queueManager.removeItem(q.id, taskId),
                        ),
                      ],
                    ),
                  );
                }),
              ],
            ),
          ),
        );
      },
    );
  }

  Widget _buildSchedulesTab() {
    final entries = _scheduler.entries;
    return ListView(
      padding: const EdgeInsets.all(12),
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              'Scheduled Tasks',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            ElevatedButton.icon(
              icon: const Icon(Icons.add_alarm),
              label: const Text('Add Schedule'),
              onPressed: _showAddScheduleDialog,
            ),
          ],
        ),
        const SizedBox(height: 12),
        if (entries.isEmpty)
          const Center(
            child: Padding(
              padding: EdgeInsets.all(32.0),
              child: Text('No active scheduled entries.'),
            ),
          ),
        ...entries.map((entry) {
          return Card(
            margin: const EdgeInsets.only(bottom: 8),
            child: ListTile(
              leading: const CircleAvatar(
                child: Icon(Icons.alarm),
              ),
              title: Text(entry.label),
              subtitle: Text(
                'Fire Time: ${entry.fireTime.toLocal()}\nAction: ${entry.action.name} (Queue: ${entry.assignedQueueId ?? "N/A"})\nRecurrence: ${entry.recurrence.name}',
              ),
              isThreeLine: true,
              trailing: IconButton(
                icon: const Icon(Icons.delete, color: Colors.red),
                onPressed: () async {
                  await _scheduler.remove(entry.id);
                  setState(() {});
                },
              ),
            ),
          );
        }),
      ],
    );
  }

  Widget _buildLogsTab() {
    return ListView.builder(
      padding: const EdgeInsets.all(12),
      itemCount: _logs.length,
      itemBuilder: (context, index) {
        return Container(
          padding: const EdgeInsets.symmetric(vertical: 4),
          decoration: BoxDecoration(
            border: Border(bottom: BorderSide(color: Colors.grey.shade200)),
          ),
          child: Text(
            _logs[index],
            style: const TextStyle(fontFamily: 'monospace', fontSize: 13),
          ),
        );
      },
    );
  }

  Widget _buildStatusChip(QueueStatus status) {
    Color color;
    switch (status) {
      case QueueStatus.running:
        color = Colors.green;
        break;
      case QueueStatus.paused:
        color = Colors.orange;
        break;
      case QueueStatus.scheduled:
        color = Colors.purple;
        break;
      case QueueStatus.idle:
        color = Colors.grey;
        break;
    }
    return Chip(
      label: Text(
        status.name.toUpperCase(),
        style: const TextStyle(color: Colors.white, fontSize: 11, fontWeight: FontWeight.bold),
      ),
      backgroundColor: color,
      padding: const EdgeInsets.symmetric(horizontal: 4),
    );
  }

  void _showAddNewQueueDialog() {
    final nameController = TextEditingController(text: 'New Queue');
    final concurrencyController = TextEditingController(text: '2');
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Add New Download Queue'),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: nameController,
              decoration: const InputDecoration(labelText: 'Queue Name'),
            ),
            TextField(
              controller: concurrencyController,
              decoration: const InputDecoration(labelText: 'Concurrency (Max Parallel)'),
              keyboardType: TextInputType.number,
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('Cancel'),
          ),
          ElevatedButton(
            onPressed: () async {
              final id = 'queue-${DateTime.now().millisecondsSinceEpoch}';
              final concurrency = int.tryParse(concurrencyController.text) ?? 2;
              await _queueManager.addQueue(
                DownloadQueue(
                  id: id,
                  name: nameController.text.trim(),
                  concurrency: concurrency,
                ),
              );
              if (context.mounted) Navigator.pop(context);
              setState(() {});
            },
            child: const Text('Create'),
          ),
        ],
      ),
    );
  }

  void _showAddTaskDialog(String queueId) {
    final urlController = TextEditingController(
      text: 'https://example.com/file-${DateTime.now().second}.zip',
    );
    final nameController = TextEditingController(
      text: 'file-${DateTime.now().second}.zip',
    );
    var priority = FluxPriority.normal;

    showDialog(
      context: context,
      builder: (context) => StatefulBuilder(
        builder: (context, setDialogState) => AlertDialog(
          title: const Text('Add Task to Queue'),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextField(
                controller: urlController,
                decoration: const InputDecoration(labelText: 'URL'),
              ),
              TextField(
                controller: nameController,
                decoration: const InputDecoration(labelText: 'File Name'),
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<FluxPriority>(
                initialValue: priority,
                decoration: const InputDecoration(labelText: 'Priority'),
                items: FluxPriority.values
                    .map((p) => DropdownMenuItem(value: p, child: Text(p.name)))
                    .toList(),
                onChanged: (val) {
                  if (val != null) setDialogState(() => priority = val);
                },
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.pop(context),
              child: const Text('Cancel'),
            ),
            ElevatedButton(
              onPressed: () async {
                await _queueManager.createTask(
                  queueId,
                  url: urlController.text.trim(),
                  fileName: nameController.text.trim(),
                  priority: priority,
                );
                if (context.mounted) Navigator.pop(context);
                setState(() {});
              },
              child: const Text('Add'),
            ),
          ],
        ),
      ),
    );
  }

  void _showEditRulesDialog(DownloadQueue queue) {
    final concurrencyController =
        TextEditingController(text: queue.concurrency.toString());
    final speedController = TextEditingController(
      text: (queue.bandwidthLimitBps ~/ (1024 * 1024)).toString(),
    );

    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: Text('Edit Rules: ${queue.name}'),
        content: Column(
          mainAxisSize: StandardizedMainAxisSize,
          children: [
            TextField(
              controller: concurrencyController,
              decoration: const InputDecoration(labelText: 'Concurrency'),
              keyboardType: TextInputType.number,
            ),
            TextField(
              controller: speedController,
              decoration: const InputDecoration(
                labelText: 'Bandwidth Limit (MB/s, 0=unlimited)',
              ),
              keyboardType: TextInputType.number,
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('Cancel'),
          ),
          ElevatedButton(
            onPressed: () async {
              final concurrency = int.tryParse(concurrencyController.text) ?? 2;
              final mbps = int.tryParse(speedController.text) ?? 0;
              await _queueManager.updateRules(
                queue.id,
                concurrency: concurrency,
                bandwidthLimitBps: mbps * 1024 * 1024,
              );
              if (context.mounted) Navigator.pop(context);
              setState(() {});
            },
            child: const Text('Save'),
          ),
        ],
      ),
    );
  }

  void _showAddScheduleDialog() {
    final labelController = TextEditingController(text: 'Night Download');
    var selectedQueue = _queueManager.queues.firstOrNull?.id;
    var recurrence = Recurrence.daily;

    showDialog(
      context: context,
      builder: (context) => StatefulBuilder(
        builder: (context, setDialogState) => AlertDialog(
          title: const Text('New Schedule Entry'),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextField(
                controller: labelController,
                decoration: const InputDecoration(labelText: 'Label'),
              ),
              const SizedBox(height: 8),
              DropdownButtonFormField<String>(
                initialValue: selectedQueue,
                decoration: const InputDecoration(labelText: 'Target Queue'),
                items: _queueManager.queues
                    .map((q) => DropdownMenuItem(value: q.id, child: Text(q.name)))
                    .toList(),
                onChanged: (val) => setDialogState(() => selectedQueue = val),
              ),
              const SizedBox(height: 8),
              DropdownButtonFormField<Recurrence>(
                initialValue: recurrence,
                decoration: const InputDecoration(labelText: 'Recurrence'),
                items: Recurrence.values
                    .map((r) => DropdownMenuItem(value: r, child: Text(r.name)))
                    .toList(),
                onChanged: (val) {
                  if (val != null) setDialogState(() => recurrence = val);
                },
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.pop(context),
              child: const Text('Cancel'),
            ),
            ElevatedButton(
              onPressed: () async {
                final fireTime = DateTime.now().add(const Duration(minutes: 1));
                await _scheduler.add(
                  ScheduleEntry(
                    id: 'sched-${DateTime.now().millisecondsSinceEpoch}',
                    label: labelController.text.trim(),
                    fireTime: fireTime,
                    recurrence: recurrence,
                    action: ScheduleAction.startQueue,
                    assignedQueueId: selectedQueue,
                  ),
                );
                if (context.mounted) Navigator.pop(context);
                setState(() {});
              },
              child: const Text('Schedule'),
            ),
          ],
        ),
      ),
    );
  }
}

const StandardizedMainAxisSize = MainAxisSize.min;
