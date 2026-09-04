import 'dart:typed_data';

/// 'ORBA'.
const int _ackMagic = 0x4F524241;

/// A client telling the authority which tick it has applied.
///
/// Without this the authority has to assume every snapshot arrived, and one
/// dropped packet desynchronises every delta after it. With it, deltas are
/// built against something the client has actually confirmed, so a loss costs
/// one larger message rather than a broken world.
Uint8List encodeAck(int tick) {
  final message = Uint8List(12);
  final data = ByteData.sublistView(message);
  data.setUint32(0, _ackMagic, Endian.little);
  data.setUint64(4, tick, Endian.little);
  return message;
}

/// The acknowledged tick, or null if this is not an ack.
int? decodeAck(Uint8List message) {
  if (message.length != 12) return null;
  final data = ByteData.sublistView(message);
  if (data.getUint32(0, Endian.little) != _ackMagic) return null;
  return data.getUint64(4, Endian.little);
}
