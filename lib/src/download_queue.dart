enum QueueStatus { idle, running, paused, scheduled }

/// Ordered group of engine task ids with queue-level execution rules.
final class DownloadQueue {
  DownloadQueue({
    required this.id,
    required this.name,
    this.status = QueueStatus.idle,
    this.concurrency = 2,
    this.bandwidthLimitBps = 0,
    List<String> itemIds = const [],
    this.startsAt,
  }) : itemIds = List.unmodifiable(itemIds) {
    if (id.isEmpty) throw ArgumentError.value(id, 'id', 'must not be empty');
    if (concurrency < 1) throw ArgumentError.value(concurrency, 'concurrency');
    if (bandwidthLimitBps < 0) {
      throw ArgumentError.value(bandwidthLimitBps, 'bandwidthLimitBps');
    }
  }

  final String id;
  final String name;
  final QueueStatus status;
  final int concurrency;
  final int bandwidthLimitBps;
  final List<String> itemIds;
  final DateTime? startsAt;

  DownloadQueue copyWith({
    String? name,
    QueueStatus? status,
    int? concurrency,
    int? bandwidthLimitBps,
    List<String>? itemIds,
    Object? startsAt = _unset,
  }) => DownloadQueue(
    id: id,
    name: name ?? this.name,
    status: status ?? this.status,
    concurrency: concurrency ?? this.concurrency,
    bandwidthLimitBps: bandwidthLimitBps ?? this.bandwidthLimitBps,
    itemIds: itemIds ?? this.itemIds,
    startsAt: identical(startsAt, _unset)
        ? this.startsAt
        : startsAt as DateTime?,
  );

  Map<String, dynamic> toJson() => {
    'id': id,
    'name': name,
    'status': status.name,
    'concurrency': concurrency,
    'bandwidthLimitBps': bandwidthLimitBps,
    'itemIds': itemIds,
    'startsAt': startsAt?.toIso8601String(),
  };

  factory DownloadQueue.fromJson(Map<String, dynamic> json) => DownloadQueue(
    id: json['id'] as String,
    name: json['name'] as String? ?? '',
    status: QueueStatus.values.firstWhere(
      (v) => v.name == json['status'],
      orElse: () => QueueStatus.idle,
    ),
    concurrency: (json['concurrency'] as num?)?.toInt() ?? 2,
    bandwidthLimitBps: (json['bandwidthLimitBps'] as num?)?.toInt() ?? 0,
    itemIds: (json['itemIds'] as List? ?? const []).cast<String>(),
    startsAt: DateTime.tryParse(json['startsAt'] as String? ?? ''),
  );
}

const Object _unset = Object();

final class QueueProgress {
  const QueueProgress({
    required this.queueId,
    required this.percent,
    required this.eta,
    required this.speedBps,
    required this.completed,
    required this.total,
  });
  final String queueId;
  final double percent;
  final Duration? eta;
  final double speedBps;
  final int completed;
  final int total;
}
