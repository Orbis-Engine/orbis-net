/// Multiplayer for Orbis.
///
/// Replication reads component columns rather than walking entities, so
/// capturing a world costs a handful of contiguous copies instead of a lookup
/// per entity.
///
/// Two transports ship with it. A loopback link, which is not only for tests —
/// a single-player build and a listen server can run the real replication path
/// over it, so the networked code path is the only code path and does not rot
/// between multiplayer sessions. And a socket transport for actual play.
/// [Transport] is a small interface, so a third is an afternoon.
library;

export 'src/ack.dart' show decodeAck, encodeAck;
export 'src/client.dart' show NetClient;
export 'src/codec.dart'
    show DecodedSnapshot, SnapshotCodec, SnapshotFormatError;
export 'src/host.dart' show InputRejection, NetHost;
export 'src/input.dart' show InputEntry, InputMessage, decodeInput, encodeInput;
export 'src/replication.dart' show ReplicatedComponent, ReplicationSet;
export 'src/snapshot.dart'
    show SnapshotCapture, SnapshotGroup, SnapshotRow, WorldSnapshot;
export 'src/socket_transport.dart' show SocketServer, SocketTransport;
export 'src/transport.dart' show LoopbackLink, Transport;
