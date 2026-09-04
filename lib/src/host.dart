import 'dart:async';
import 'dart:typed_data';

import 'package:orbis_core/orbis_core.dart';

import 'ack.dart';
import 'codec.dart';
import 'input.dart';
import 'replication.dart';
import 'snapshot.dart';
import 'transport.dart';

/// One connected peer, from the authority's side.
class _Client {
  _Client(this.id, this.index, this.transport, this.subscription);

  final String id;

  /// A small number for this client, written into the owner component so a
  /// peer can tell which entities are its own.
  final int index;

  final Transport transport;
  final StreamSubscription<Uint8List> subscription;

  /// The last tick this client confirmed applying. Deltas are built against it,
  /// so a client that falls silent gets larger messages rather than a world
  /// that drifts.
  int acknowledgedTick = 0;
}

/// Why an input was not applied. Counted rather than thrown: a hostile client
/// should not be able to raise exceptions on the authority.
enum InputRejection {
  /// No entity with that network id.
  unknownEntity,

  /// The client does not own the entity it addressed.
  notOwned,

  /// The client tried to write a component the authority reserves.
  notWritable,

  /// The payload did not match the components it claimed to carry.
  malformed,
}

/// The authority: it owns the world and tells clients what happened.
class NetHost {
  NetHost({
    required World world,
    required this.set,
    required ComponentType networkId,
    ComponentType? ownerComponent,
    this.historyDepth = 64,
  }) : _world = world,
       _networkId = networkId,
       _ownerComponent = ownerComponent,
       _capture = SnapshotCapture(
         world: world,
         set: set,
         networkId: networkId,
       ) {
    var writable = 0;
    for (final component in set.components) {
      if (component.ownerWritable) writable |= 1 << component.bit;
    }
    _ownerWritableMask = writable;
  }

  final World _world;
  final ReplicationSet set;
  final ComponentType _networkId;

  /// Optional component holding the owning client's index, so ownership can
  /// replicate and a client can recognise the entities it drives.
  final ComponentType? _ownerComponent;

  final SnapshotCapture _capture;
  final SnapshotCodec _codec = const SnapshotCodec();

  /// How many past snapshots to keep for building deltas. A client further
  /// behind than this is sent a full snapshot instead.
  final int historyDepth;

  late final int _ownerWritableMask;

  final Map<String, _Client> _clients = {};
  final Map<int, WorldSnapshot> _history = {};
  final List<int> _historyOrder = [];
  final Map<int, int> _entityByNetworkId = {};
  final Map<int, String> _ownerByNetworkId = {};

  final Map<InputRejection, int> _rejections = {};
  int _acceptedInputs = 0;

  int _nextNetworkId = 1;
  int _nextClientIndex = 1;
  int _tick = 0;

  World get world => _world;
  int get tick => _tick;
  int get clientCount => _clients.length;

  /// Inputs applied since start, and why others were not. Worth surfacing:
  /// a client that is being refused constantly is either out of date or
  /// probing, and both are things an operator wants to see.
  int get acceptedInputs => _acceptedInputs;
  Map<InputRejection, int> get rejections => Map.unmodifiable(_rejections);

  /// Marks [entity] as replicated and gives it a wire identity.
  int spawn(int entity, {String? owner}) {
    final networkId = _nextNetworkId++;
    _world.add(entity, _networkId, Uint64List(1)..[0] = networkId);
    _entityByNetworkId[networkId] = entity;
    if (_ownerComponent != null) _world.add(entity, _ownerComponent);
    if (owner != null) setOwner(networkId, owner);
    return networkId;
  }

  /// Hands an entity to a client, or back to the authority with null.
  void setOwner(int networkId, String? clientId) {
    if (clientId == null) {
      _ownerByNetworkId.remove(networkId);
    } else {
      _ownerByNetworkId[networkId] = clientId;
    }

    final entity = _entityByNetworkId[networkId];
    final component = _ownerComponent;
    if (entity == null || component == null) return;
    if (!_world.isAlive(entity) || !_world.has(entity, component)) return;

    final index = clientId == null ? 0 : (_clients[clientId]?.index ?? 0);
    final bytes = _world.bytesOf(entity, component)!;
    ByteData.sublistView(bytes).setUint32(0, index, Endian.little);
  }

  String? ownerOf(int networkId) => _ownerByNetworkId[networkId];

  /// The authority's own entity for a network id, if it still exists.
  int? entityFor(int networkId) => _entityByNetworkId[networkId];

  /// Stops replicating [entity] and destroys it. Clients learn on the next
  /// publish, from its absence.
  void despawn(int entity) => _world.destroyEntity(entity);

  void addClient(String id, Transport transport) {
    if (_clients.containsKey(id)) {
      throw StateError('A client is already connected as "$id".');
    }
    late final _Client client;
    final subscription = transport.inbound.listen(
      (message) => _receive(client, message),
    );
    client = _Client(id, _nextClientIndex++, transport, subscription);
    _clients[id] = client;
  }

  Future<void> removeClient(String id) async {
    final client = _clients.remove(id);
    if (client == null) return;
    await client.subscription.cancel();
    // Entities the departed client owned revert to the authority rather than
    // staying writable by a name that could be reconnected under.
    for (final networkId
        in _ownerByNetworkId.entries
            .where((entry) => entry.value == id)
            .map((entry) => entry.key)
            .toList()) {
      setOwner(networkId, null);
    }
  }

  void _receive(_Client client, Uint8List message) {
    final acknowledged = decodeAck(message);
    if (acknowledged != null) {
      if (acknowledged > client.acknowledgedTick) {
        client.acknowledgedTick = acknowledged;
      }
      return;
    }

    final input = decodeInput(message);
    if (input != null) _applyInput(client, input);
  }

  /// Applies what a client asked for, to the extent it is allowed to.
  ///
  /// Every refusal here is a rule, not an inconvenience: ownership decides who
  /// may write which components, so this is the surface a hostile client
  /// probes first.
  void _applyInput(_Client client, InputMessage input) {
    for (final entry in input.entries) {
      final entity = _entityByNetworkId[entry.networkId];
      if (entity == null || !_world.isAlive(entity)) {
        _reject(InputRejection.unknownEntity);
        continue;
      }
      if (_ownerByNetworkId[entry.networkId] != client.id) {
        _reject(InputRejection.notOwned);
        continue;
      }
      if (entry.mask & ~_ownerWritableMask != 0) {
        _reject(InputRejection.notWritable);
        continue;
      }
      if (entry.row.length != set.strideOf(entry.mask)) {
        _reject(InputRejection.malformed);
        continue;
      }

      var offset = 0;
      for (final bit in set.bitsOf(entry.mask)) {
        final type = set.atBit(bit).type;
        final target = _world.bytesOf(entity, type);
        // A client may name a component the entity does not have; that is
        // stale rather than hostile, so the rest of the entry still applies.
        target?.setRange(0, type.byteSize, entry.row, offset);
        offset += type.byteSize;
      }
      _acceptedInputs++;
    }
  }

  void _reject(InputRejection reason) {
    _rejections[reason] = (_rejections[reason] ?? 0) + 1;
  }

  /// Captures the world and sends each client what it has not seen.
  ///
  /// The capture happens once no matter how many clients are connected; only
  /// the diff is per client, and that is a comparison rather than a re-read.
  void publish() {
    _tick++;
    final current = _capture.capture(_tick);

    for (final client in _clients.values) {
      final baseline = _history[client.acknowledgedTick];
      final message = baseline == null
          ? _codec.encodeFull(current)
          : _codec.encodeDelta(current, baseline);
      client.transport.send(message);
    }

    _remember(current);
    _forgetDeadEntities(current);
  }

  void _forgetDeadEntities(WorldSnapshot current) {
    if (_entityByNetworkId.length == current.entityCount) return;
    for (final networkId in _entityByNetworkId.keys.toList()) {
      if (current.rowFor(networkId) == null) {
        _entityByNetworkId.remove(networkId);
        _ownerByNetworkId.remove(networkId);
      }
    }
  }

  void _remember(WorldSnapshot snapshot) {
    _history[snapshot.tick] = snapshot;
    _historyOrder.add(snapshot.tick);
    while (_historyOrder.length > historyDepth) {
      _history.remove(_historyOrder.removeAt(0));
    }
  }

  Future<void> dispose() async {
    for (final client in _clients.values) {
      await client.subscription.cancel();
    }
    _clients.clear();
    _capture.dispose();
  }
}
