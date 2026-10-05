# queue_scheduler

Pure Dart queue execution and scheduling for `download_engine`. It has no
Flutter imports or GetX dependency and can be consumed by Flutter applications
on Windows, macOS, and Linux.

```yaml
dependencies:
  queue_scheduler:
    path: ./queue_scheduler
```

Tasks should be created through `QueueManager.createTask` or added to the
engine with `autoStart: false`. This lets the manager control starts. Queue
bandwidth is divided evenly among active tasks in that queue and applied as
per-task engine limits. The engine's own global bandwidth setting can still
provide an additional overall cap.

`QueueManager.restore()` restores queue membership. `TaskScheduler.restore()`
restores upcoming entries and emits `missed` events for due times missed while
the application was stopped. Use a dedicated `JsonQueueSchedulerStore` JSON
file for queues and schedules; it uses `download_persistence`'s `TaskStore`
interface and JSON backend. Keep that metadata store separate from the
download task registry.

`ScheduleAction.custom` callbacks are deliberately host-owned. Use
`onCustomAction` (or an entry callback) to connect actions such as
`shutdownSystem` and `sleepSystem` to the host application's OS integration.

Clock and timer factories are injectable. Queue manager tests can use
`QueueManager.withEngine` and a fake `QueueDownloadEngine`.
