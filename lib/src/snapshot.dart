import 'dart:typed_data';

import 'package:orbis_core/orbis_core.dart';

import 'replication.dart';

/// One run of entities sharing the same set of replicated components.
///
/// Rows are interleaved — an entity's components sit together — because that
/// is the shape a delta sends and the shape a comparison wants. The column
/// layout still pays for itself on the way in: gathering a group reads each
/// component's storage in one contiguous pass, so capturing five hundred
/// transforms is a handful of bulk copies rather than five hundred lookups.
class SnapshotGroup {
  SnapshotGroup({
    required this.mask,
    required this.stride,
    required this.networkIds,
    required this.rows,
  });

  /// Which replicated components these entities carry.
  final int mask;

  /// Bytes per entity under [mask].
  final int stride;

  final Uint64List networkIds;

  /// `networkIds.length * stride` bytes, entity by entity.
  final Uint8List rows;

  int get length => networkIds.length;

  Uint8List rowAt(int index) =>
      Uint8List.sublistView(rows, index * stride, (index + 1) * stride);
}

/// Where one entity's state sits inside a snapshot.
class SnapshotRow {
  const SnapshotRow(this.mask, this.group, this.index);

  final int mask;
  final SnapshotGroup group;
  final int index;

  Uint8List get bytes => group.rowAt(index);
}

/// Everything replicated, as of one tick.
class WorldSnapshot {
  WorldSnapshot(this.tick, this.groups)
    : _byNetworkId = {
        for (final group in groups)
          for (var i = 0; i < group.length; i++)
            group.networkIds[i]: SnapshotRow(group.mask, group, i),
      };

  final int tick;
  final List<SnapshotGroup> groups;
  final Map<int, SnapshotRow> _byNetworkId;

  Iterable<int> get networkIds => _byNetworkId.keys;

  SnapshotRow? rowFor(int networkId) => _byNetworkId[networkId];

  int get entityCount => _byNetworkId.length;

  static WorldSnapshot empty(int tick) => WorldSnapshot(tick, const []);
}

/// Reads the replicated state of a world.
///
/// Holds its query across ticks so the matching archetypes are resolved once
/// rather than every frame.
class SnapshotCapture {
  SnapshotCapture({
    required World world,
    required this.set,
    required this.networkId,
  }) : _world = world,
       _query = world.query([networkId]) {
    if (set.bitOf(networkId) != null) {
      throw ArgumentError(
        'The network id must not be part of the replication set: it keys '
        'the rows rather than travelling inside them.',
      );
    }
  }

  final World _world;
  final ReplicationSet set;

  /// The component holding each replicated entity's stable wire identity.
  final ComponentType networkId;

  final Query _query;

  WorldSnapshot capture(int tick) {
    final groups = <SnapshotGroup>[];

    for (final chunk in _query.chunks) {
      final mask = set.maskOfIds(chunk.componentIds);
      if (mask == 0) continue;

      final length = chunk.length;
      final stride = set.strideOf(mask);
      final rows = Uint8List(length * stride);

      var offset = 0;
      for (final bit in set.bitsOf(mask)) {
        final type = set.atBit(bit).type;
        final size = type.byteSize;
        final column = chunk.bytesOfComponent(type)!;
        for (var row = 0; row < length; row++) {
          rows.setRange(
            row * stride + offset,
            row * stride + offset + size,
            column,
            row * size,
          );
        }
        offset += size;
      }

      groups.add(
        SnapshotGroup(
          mask: mask,
          stride: stride,
          networkIds: _readNetworkIds(chunk, length),
          rows: rows,
        ),
      );
    }

    return WorldSnapshot(tick, groups);
  }

  Uint64List _readNetworkIds(Chunk chunk, int length) {
    final bytes = chunk.bytesOfComponent(networkId)!;
    // The column comes from the allocator at max_align_t, and asTypedList
    // starts at offset zero, so an eight-byte view over it is aligned.
    final view = Uint64List.view(bytes.buffer, bytes.offsetInBytes, length);
    return Uint64List.fromList(view);
  }

  void dispose() => _query.dispose();

  World get world => _world;
}
