import 'dart:typed_data';

/// 'ORBI'.
const int _inputMagic = 0x4F524249;

/// One entity a client is proposing to change.
class InputEntry {
  const InputEntry({
    required this.networkId,
    required this.mask,
    required this.row,
  });

  final int networkId;

  /// Which components the client is writing, as bits in the replication set.
  final int mask;

  /// The component bytes, in ascending bit order, exactly as a snapshot row.
  final Uint8List row;
}

/// A client's proposed writes to entities it owns.
class InputMessage {
  const InputMessage({required this.tick, required this.entries});

  /// The client's tick when it produced this, so the authority can tell how
  /// stale the input is.
  final int tick;

  final List<InputEntry> entries;
}

Uint8List encodeInput(InputMessage message) {
  var size = 14;
  for (final entry in message.entries) {
    size += 20 + entry.row.length;
  }

  final bytes = Uint8List(size);
  final data = ByteData.sublistView(bytes);
  data.setUint32(0, _inputMagic, Endian.little);
  data.setUint64(4, message.tick, Endian.little);
  data.setUint16(12, message.entries.length, Endian.little);

  var offset = 14;
  for (final entry in message.entries) {
    data.setUint64(offset, entry.networkId, Endian.little);
    data.setUint64(offset + 8, entry.mask, Endian.little);
    data.setUint32(offset + 16, entry.row.length, Endian.little);
    offset += 20;
    bytes.setRange(offset, offset + entry.row.length, entry.row);
    offset += entry.row.length;
  }
  return bytes;
}

/// Decodes an input message, or returns null if this is something else.
InputMessage? decodeInput(Uint8List bytes) {
  if (bytes.length < 14) return null;
  final data = ByteData.sublistView(bytes);
  if (data.getUint32(0, Endian.little) != _inputMagic) return null;

  final tick = data.getUint64(4, Endian.little);
  final count = data.getUint16(12, Endian.little);
  final entries = <InputEntry>[];

  var offset = 14;
  for (var i = 0; i < count; i++) {
    if (offset + 20 > bytes.length) return null;
    final networkId = data.getUint64(offset, Endian.little);
    final mask = data.getUint64(offset + 8, Endian.little);
    final length = data.getUint32(offset + 16, Endian.little);
    offset += 20;
    if (offset + length > bytes.length) return null;
    entries.add(
      InputEntry(
        networkId: networkId,
        mask: mask,
        row: Uint8List.fromList(
          Uint8List.sublistView(bytes, offset, offset + length),
        ),
      ),
    );
    offset += length;
  }

  return InputMessage(tick: tick, entries: entries);
}
