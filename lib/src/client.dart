import 'dart:async';
import 'dart:typed_data';

import 'package:orbis_core/orbis_core.dart';

import 'ack.dart';
import 'codec.dart';
import 'input.dart';
import 'replication.dart';
import 'transport.dart';

/// One entity's replicated state, as a complete row.
class _EntityState {
  const _EntityState(this.mask, this.row);

  final int mask;
  final Uint8List row;
}

/// Everything the authority had said as of one tick.
///
/// Deltas describe change, not state, so the client rebuilds the whole picture
/// as messages arrive. That is what makes interpolation possible at all: you
/// cannot blend towards a frame you only have the difference to.
class _WorldState {
  _WorldState(this.tick, this.receivedAt, this.entities);

  final int tick;
  final double receivedAt;
  final Map<int, _EntityState> entities;
}

/// The replica side: it receives what the authority did and reproduces it.
///
/// The client's world is its own, with its own component ids. Nothing on the
/// wire refers to those — components are identified by their position in the
/// replication set, which is derived from their names. Two builds that declare
/// the same components therefore agree without exchanging a schema, even if
/// they registered them in different orders.
class NetClient {
  NetClient({
    required World world,
    required this.set,
    required ComponentType networkId,
    required Transport transport,
    this.interpolationDelay = Duration.zero,
    this.historyDepth = 32,
    this.onSpawn,
    this.onDespawn,
    double Function()? clock,
  }) : _world = world,
       _networkId = networkId,
       _transport = transport,
       _now = clock ?? _stopwatchClock() {
    _subscription = transport.inbound.listen(_receive);
  }

  /// Seconds since this client started, unless the caller supplies its own.
  /// A game usually should: the frame clock it already keeps is a better time
  /// base than a second stopwatch drifting alongside it.
  static double Function() _stopwatchClock() {
    final stopwatch = Stopwatch()..start();
    return () => stopwatch.elapsedMicroseconds / 1e6;
  }

  final World _world;
  final ReplicationSet set;
  final ComponentType _networkId;
  final Transport _transport;
  final SnapshotCodec _codec = const SnapshotCodec();

  /// How far behind the authority to render.
  ///
  /// Zero applies each snapshot the moment it lands, which is correct and
  /// looks wrong: remote entities step at the authority's tick rate rather
  /// than moving. A delay of roughly two ticks buys a state on either side of
  /// the render time to blend between, at the cost of showing the world
  /// slightly late — which is the trade every networked game makes.
  final Duration interpolationDelay;

  /// How many past states to keep for blending.
  final int historyDepth;

  final void Function(int networkId, int entity)? onSpawn;
  final void Function(int networkId, int entity)? onDespawn;

  late final StreamSubscription<Uint8List> _subscription;
  final double Function() _now;
  final Map<int, int> _entities = {};
  final List<_WorldState> _history = [];

  int _lastAppliedTick = 0;

  World get world => _world;

  bool get isInterpolating => interpolationDelay > Duration.zero;

  /// The last tick received in full. Sent back to the authority so its deltas
  /// are built against something this client has actually seen.
  int get lastAppliedTick => _lastAppliedTick;

  int get entityCount => _entities.length;

  int? entityFor(int networkId) => _entities[networkId];

  Iterable<int> get networkIds => _entities.keys;

  /// Proposes writes to entities this client owns.
  ///
  /// The authority decides whether to honour them: only components declared
  /// owner-writable, and only on entities it agrees this client owns. Anything
  /// else is counted and dropped there rather than trusted here.
  void sendInput(Map<int, Map<ComponentType, TypedData>> writes) {
    final entries = <InputEntry>[];

    for (final entity in writes.entries) {
      final components = entity.value;
      var mask = 0;
      for (final type in components.keys) {
        final bit = set.bitOf(type);
        if (bit == null) {
          throw ArgumentError('${type.name} is not a replicated component.');
        }
        mask |= 1 << bit;
      }

      final row = Uint8List(set.strideOf(mask));
      var offset = 0;
      for (final bit in set.bitsOf(mask)) {
        final type = set.atBit(bit).type;
        final value = components[type]!;
        row.setRange(
          offset,
          offset + type.byteSize,
          value.buffer.asUint8List(value.offsetInBytes, type.byteSize),
        );
        offset += type.byteSize;
      }

      entries.add(InputEntry(networkId: entity.key, mask: mask, row: row));
    }

    _transport.send(
      encodeInput(InputMessage(tick: _lastAppliedTick, entries: entries)),
    );
  }

  void _receive(Uint8List message) {
    final snapshot = _codec.decode(message);

    // A delta against a tick this client never applied would leave the world
    // partly stale in a way nothing later corrects. Acknowledging honestly
    // instead makes the authority send a full snapshot next time.
    if (snapshot.isDelta && snapshot.baselineTick != _lastAppliedTick) {
      _transport.send(encodeAck(_lastAppliedTick));
      return;
    }

    final state = _merge(snapshot);
    _history.add(state);
    while (_history.length > historyDepth) {
      _history.removeAt(0);
    }

    if (!isInterpolating) _applyState(state);

    _lastAppliedTick = snapshot.tick;
    _transport.send(encodeAck(_lastAppliedTick));
  }

  /// Folds a snapshot into the previous complete state.
  _WorldState _merge(DecodedSnapshot snapshot) {
    final previous = _history.isEmpty ? null : _history.last;
    final entities = <int, _EntityState>{
      // A full snapshot is the whole truth, so it starts from nothing; a delta
      // says nothing about the entities it omits, so it starts from what stood.
      if (snapshot.isDelta && previous != null) ...previous.entities,
    };

    for (final group in snapshot.groups) {
      for (var i = 0; i < group.length; i++) {
        entities[group.networkIds[i]] = _EntityState(
          group.mask,
          group.rowAt(i),
        );
      }
    }
    for (final networkId in snapshot.despawned) {
      entities.remove(networkId);
    }

    return _WorldState(snapshot.tick, _now(), entities);
  }

  /// Blends the world towards where the authority was, [interpolationDelay]
  /// ago. Call once per frame; a no-op when not interpolating.
  void advance() {
    if (!isInterpolating || _history.isEmpty) return;

    final target = _now() - interpolationDelay.inMicroseconds / 1e6;

    // Behind everything held: show the oldest rather than extrapolating into
    // a future the authority has not described.
    if (target <= _history.first.receivedAt) {
      _applyState(_history.first);
      return;
    }
    // Ahead of everything held — the authority has gone quiet. Holding the
    // newest state is honest; guessing would invent motion that never happened.
    if (target >= _history.last.receivedAt) {
      _applyState(_history.last);
      return;
    }

    var index = 0;
    for (var i = 0; i < _history.length - 1; i++) {
      if (_history[i + 1].receivedAt > target) {
        index = i;
        break;
      }
    }

    final from = _history[index];
    final to = _history[index + 1];
    final span = to.receivedAt - from.receivedAt;
    final t = span <= 0
        ? 1.0
        : ((target - from.receivedAt) / span).clamp(0.0, 1.0);
    _applyState(from, blendTowards: to, t: t);
  }

  void _applyState(
    _WorldState state, {
    _WorldState? blendTowards,
    double t = 0,
  }) {
    for (final entry in state.entities.entries) {
      final other = blendTowards?.entities[entry.key];
      _applyEntity(
        entry.key,
        entry.value,
        // Blending only makes sense between two descriptions of the same
        // thing; a component set that changed mid-blend is a structural event,
        // and the earlier state is the one that is safe to show.
        (other != null && other.mask == entry.value.mask) ? other : null,
        t,
      );
    }

    for (final networkId in _entities.keys.toList()) {
      if (!state.entities.containsKey(networkId)) _despawn(networkId);
    }
  }

  void _applyEntity(
    int networkId,
    _EntityState state,
    _EntityState? towards,
    double t,
  ) {
    var entity = _entities[networkId];
    final isNew = entity == null;
    if (entity == null) {
      entity = _world.createEntity();
      _world.add(entity, _networkId, Uint64List(1)..[0] = networkId);
      _entities[networkId] = entity;
    }

    // The component set is reconciled before anything is written, because
    // adding or removing one moves the entity between archetypes and
    // invalidates every view taken before the move.
    for (final component in set.components) {
      final wanted = state.mask & (1 << component.bit) != 0;
      final present = _world.has(entity, component.type);
      if (wanted && !present) {
        _world.add(entity, component.type);
      } else if (!wanted && present) {
        _world.remove(entity, component.type);
      }
    }

    var offset = 0;
    for (final bit in set.bitsOf(state.mask)) {
      final type = set.atBit(bit).type;
      final size = type.byteSize;
      final target = _world.bytesOf(entity, type)!;

      if (towards != null && type.kind == ComponentKind.float32) {
        // Positions and rotations are what motion is made of, so they blend.
        // Anything else — health, flags, ids — takes the earlier value, since
        // a half-applied integer is not a smaller error than a late one.
        final a = ByteData.sublistView(state.row, offset, offset + size);
        final b = ByteData.sublistView(towards.row, offset, offset + size);
        final out = ByteData.sublistView(target, 0, size);
        for (var e = 0; e < type.arity; e++) {
          final from = a.getFloat32(e * 4, Endian.little);
          final to = b.getFloat32(e * 4, Endian.little);
          out.setFloat32(e * 4, from + (to - from) * t, Endian.little);
        }
      } else {
        target.setRange(0, size, state.row, offset);
      }
      offset += size;
    }

    if (isNew) onSpawn?.call(networkId, entity);
  }

  void _despawn(int networkId) {
    final entity = _entities.remove(networkId);
    if (entity == null) return;
    onDespawn?.call(networkId, entity);
    _world.destroyEntity(entity);
  }

  Future<void> dispose() => _subscription.cancel();
}
