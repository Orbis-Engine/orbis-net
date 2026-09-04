import 'dart:typed_data';

import 'snapshot.dart';

/// 'ORBN'.
const int _magic = 0x4F52424E;
const int _formatVersion = 1;

const int _kindFull = 0;
const int _kindDelta = 1;

/// Thrown when a message is not a snapshot this build can read.
class SnapshotFormatError extends FormatException {
  SnapshotFormatError(super.message);
}

/// A snapshot read back off the wire.
class DecodedSnapshot {
  const DecodedSnapshot({
    required this.tick,
    required this.baselineTick,
    required this.isDelta,
    required this.groups,
    required this.despawned,
  });

  final int tick;

  /// The tick this delta was built against. Zero for a full snapshot.
  final int baselineTick;

  final bool isDelta;
  final List<SnapshotGroup> groups;
  final Uint64List despawned;
}

/// Encodes and decodes snapshots.
///
/// Component bytes cross the wire exactly as the engine stores them, so a
/// transform costs twelve bytes and no conversion. That assumes both ends
/// agree on layout, which they do when they run the same build — and it
/// assumes little-endian peers, which every platform Orbis targets is. A
/// mixed-endian pairing would need byte swapping per component and is not
/// supported rather than silently wrong.
class SnapshotCodec {
  const SnapshotCodec();

  Uint8List encodeFull(WorldSnapshot snapshot) => _encode(
    tick: snapshot.tick,
    baselineTick: 0,
    isDelta: false,
    groups: snapshot.groups.where((group) => group.length > 0).toList(),
    despawned: Uint64List(0),
  );

  /// Encodes only what moved since [baseline].
  ///
  /// Most entities in most frames are unchanged, so this is where the
  /// bandwidth actually goes. Comparison is one memory compare per entity
  /// because a row is contiguous.
  Uint8List encodeDelta(WorldSnapshot current, WorldSnapshot baseline) {
    final builders = <int, _GroupBuilder>{};

    for (final group in current.groups) {
      for (var i = 0; i < group.length; i++) {
        final networkId = group.networkIds[i];
        final row = group.rowAt(i);
        final previous = baseline.rowFor(networkId);

        final changed =
            previous == null ||
            previous.mask != group.mask ||
            !_sameBytes(previous.bytes, row);
        if (!changed) continue;

        (builders[group.mask] ??= _GroupBuilder(
          group.mask,
          group.stride,
        )).add(networkId, row);
      }
    }

    final despawned = <int>[
      for (final networkId in baseline.networkIds)
        if (current.rowFor(networkId) == null) networkId,
    ];

    return _encode(
      tick: current.tick,
      baselineTick: baseline.tick,
      isDelta: true,
      groups: [for (final builder in builders.values) builder.build()],
      despawned: Uint64List.fromList(despawned),
    );
  }

  DecodedSnapshot decode(Uint8List message) {
    final data = ByteData.sublistView(message);
    if (message.length < 26) {
      throw SnapshotFormatError('Snapshot is too short to hold a header.');
    }
    if (data.getUint32(0, Endian.little) != _magic) {
      throw SnapshotFormatError('Not a snapshot.');
    }
    final version = data.getUint16(4, Endian.little);
    if (version != _formatVersion) {
      throw SnapshotFormatError(
        'Snapshot format $version, but this build speaks $_formatVersion.',
      );
    }

    final kind = data.getUint8(6);
    final tick = data.getUint64(8, Endian.little);
    final baselineTick = data.getUint64(16, Endian.little);
    final groupCount = data.getUint16(24, Endian.little);

    var offset = 26;
    final groups = <SnapshotGroup>[];

    for (var g = 0; g < groupCount; g++) {
      final mask = data.getUint64(offset, Endian.little);
      final rowCount = data.getUint32(offset + 8, Endian.little);
      final stride = data.getUint32(offset + 12, Endian.little);
      offset += 16;

      final networkIds = Uint64List(rowCount);
      for (var i = 0; i < rowCount; i++) {
        networkIds[i] = data.getUint64(offset + i * 8, Endian.little);
      }
      offset += rowCount * 8;

      final byteCount = rowCount * stride;
      if (offset + byteCount > message.length) {
        throw SnapshotFormatError('Snapshot ends inside group $g.');
      }
      final rows = Uint8List.fromList(
        Uint8List.sublistView(message, offset, offset + byteCount),
      );
      offset += byteCount;

      groups.add(
        SnapshotGroup(
          mask: mask,
          stride: stride,
          networkIds: networkIds,
          rows: rows,
        ),
      );
    }

    final despawnCount = data.getUint32(offset, Endian.little);
    offset += 4;
    final despawned = Uint64List(despawnCount);
    for (var i = 0; i < despawnCount; i++) {
      despawned[i] = data.getUint64(offset + i * 8, Endian.little);
    }

    return DecodedSnapshot(
      tick: tick,
      baselineTick: baselineTick,
      isDelta: kind == _kindDelta,
      groups: groups,
      despawned: despawned,
    );
  }

  Uint8List _encode({
    required int tick,
    required int baselineTick,
    required bool isDelta,
    required List<SnapshotGroup> groups,
    required Uint64List despawned,
  }) {
    var size = 26;
    for (final group in groups) {
      size += 16 + group.length * 8 + group.rows.length;
    }
    size += 4 + despawned.length * 8;

    final message = Uint8List(size);
    final data = ByteData.sublistView(message);

    data.setUint32(0, _magic, Endian.little);
    data.setUint16(4, _formatVersion, Endian.little);
    data.setUint8(6, isDelta ? _kindDelta : _kindFull);
    data.setUint8(7, 0);
    data.setUint64(8, tick, Endian.little);
    data.setUint64(16, baselineTick, Endian.little);
    data.setUint16(24, groups.length, Endian.little);

    var offset = 26;
    for (final group in groups) {
      data.setUint64(offset, group.mask, Endian.little);
      data.setUint32(offset + 8, group.length, Endian.little);
      data.setUint32(offset + 12, group.stride, Endian.little);
      offset += 16;

      for (var i = 0; i < group.length; i++) {
        data.setUint64(offset + i * 8, group.networkIds[i], Endian.little);
      }
      offset += group.length * 8;

      message.setRange(offset, offset + group.rows.length, group.rows);
      offset += group.rows.length;
    }

    data.setUint32(offset, despawned.length, Endian.little);
    offset += 4;
    for (var i = 0; i < despawned.length; i++) {
      data.setUint64(offset + i * 8, despawned[i], Endian.little);
    }

    return message;
  }

  static bool _sameBytes(Uint8List a, Uint8List b) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }
}

class _GroupBuilder {
  _GroupBuilder(this.mask, this.stride);

  final int mask;
  final int stride;
  final List<int> _networkIds = [];
  final BytesBuilder _rows = BytesBuilder(copy: true);

  void add(int networkId, Uint8List row) {
    _networkIds.add(networkId);
    _rows.add(row);
  }

  SnapshotGroup build() => SnapshotGroup(
    mask: mask,
    stride: stride,
    networkIds: Uint64List.fromList(_networkIds),
    rows: _rows.toBytes(),
  );
}
