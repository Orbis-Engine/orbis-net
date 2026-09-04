import 'dart:typed_data';

import 'package:orbis_core/orbis_core.dart';
import 'package:orbis_net/orbis_net.dart';
import 'package:test/test.dart';

/// Lets the loopback link deliver everything queued.
Future<void> settle() => Future<void>.delayed(Duration.zero);

/// One side of a session: a world plus the components it replicates.
class Peer {
  Peer({
    required bool reverseRegistrationOrder,
    bool includeOwner = false,
    Set<String> ownerWritable = const {},
  }) {
    world = World();
    // Registering in opposite orders on the two peers proves the wire does not
    // depend on local component ids.
    if (reverseRegistrationOrder) {
      health = world.registerComponent('Health', kind: ComponentKind.int32);
      velocity = world.registerComponent(
        'Velocity',
        kind: ComponentKind.float32,
        arity: 3,
      );
      position = world.registerComponent(
        'Position',
        kind: ComponentKind.float32,
        arity: 3,
      );
      networkId = world.registerComponent(
        'NetworkId',
        kind: ComponentKind.int64,
      );
    } else {
      networkId = world.registerComponent(
        'NetworkId',
        kind: ComponentKind.int64,
      );
      position = world.registerComponent(
        'Position',
        kind: ComponentKind.float32,
        arity: 3,
      );
      velocity = world.registerComponent(
        'Velocity',
        kind: ComponentKind.float32,
        arity: 3,
      );
      health = world.registerComponent('Health', kind: ComponentKind.int32);
    }
    owner = world.registerComponent('Owner', kind: ComponentKind.uint32);
    set = ReplicationSet([
      position,
      velocity,
      health,
      if (includeOwner) owner,
    ], ownerWritable: ownerWritable);
  }

  late final World world;
  late final ComponentType networkId;
  late final ComponentType position;
  late final ComponentType velocity;
  late final ComponentType health;
  late final ComponentType owner;
  late final ReplicationSet set;

  void dispose() => world.dispose();
}

void main() {
  group('ReplicationSet', () {
    test('orders by name so both ends agree without a handshake', () {
      final a = Peer(reverseRegistrationOrder: false);
      final b = Peer(reverseRegistrationOrder: true);
      addTearDown(a.dispose);
      addTearDown(b.dispose);

      expect(a.set.components.map((c) => c.type.name), [
        'Health',
        'Position',
        'Velocity',
      ]);
      expect(b.set.components.map((c) => c.type.name), [
        'Health',
        'Position',
        'Velocity',
      ]);
      expect(
        a.set.bitOf(a.position),
        b.set.bitOf(b.position),
        reason: 'the same component must occupy the same bit on both peers',
      );
      // ...even though the local ids differ.
      expect(a.position.id, isNot(b.position.id));
    });

    test('refuses a duplicate component', () {
      final peer = Peer(reverseRegistrationOrder: false);
      addTearDown(peer.dispose);
      expect(
        () => ReplicationSet([peer.position, peer.position]),
        throwsArgumentError,
      );
    });

    test('computes a stride from a mask', () {
      final peer = Peer(reverseRegistrationOrder: false);
      addTearDown(peer.dispose);
      final mask = peer.set.maskOfIds([peer.position.id, peer.health.id]);
      expect(peer.set.strideOf(mask), 12 + 4);
    });
  });

  group('capture and codec', () {
    late Peer host;
    late SnapshotCapture capture;

    setUp(() {
      host = Peer(reverseRegistrationOrder: false);
      capture = SnapshotCapture(
        world: host.world,
        set: host.set,
        networkId: host.networkId,
      );
    });

    tearDown(() {
      capture.dispose();
      host.dispose();
    });

    test('the network id may not be inside the replication set', () {
      final set = ReplicationSet([host.position, host.networkId]);
      expect(
        () => SnapshotCapture(
          world: host.world,
          set: set,
          networkId: host.networkId,
        ),
        throwsArgumentError,
      );
    });

    test('captures only replicated entities', () {
      final replicated = host.world.createEntity();
      host.world.add(replicated, host.networkId, Uint64List.fromList([7]));
      host.world.add(
        replicated,
        host.position,
        Float32List.fromList([1, 2, 3]),
      );

      final ignored = host.world.createEntity();
      host.world.add(ignored, host.position, Float32List.fromList([9, 9, 9]));

      final snapshot = capture.capture(1);
      expect(snapshot.entityCount, 1);
      expect(snapshot.networkIds, [7]);
    });

    test('a full snapshot round-trips', () {
      final entity = host.world.createEntity();
      host.world.add(entity, host.networkId, Uint64List.fromList([42]));
      host.world.add(entity, host.position, Float32List.fromList([1, 2, 3]));
      host.world.add(entity, host.health, Int32List.fromList([80]));

      const codec = SnapshotCodec();
      final decoded = codec.decode(codec.encodeFull(capture.capture(5)));

      expect(decoded.tick, 5);
      expect(decoded.isDelta, isFalse);
      expect(decoded.groups, hasLength(1));
      expect(decoded.groups.first.networkIds, [42]);
      // Health sorts before Position, so the row is health then position.
      final row = ByteData.sublistView(decoded.groups.first.rowAt(0));
      expect(row.getInt32(0, Endian.little), 80);
      expect(row.getFloat32(4, Endian.little), 1);
      expect(row.getFloat32(12, Endian.little), 3);
    });

    test('a delta carries only what changed', () {
      final entities = <int>[];
      for (var i = 0; i < 5; i++) {
        final entity = host.world.createEntity();
        host.world.add(entity, host.networkId, Uint64List.fromList([i + 1]));
        host.world.add(
          entity,
          host.position,
          Float32List.fromList([i * 1.0, 0, 0]),
        );
        entities.add(entity);
      }

      final baseline = capture.capture(1);
      host.world.float32Of(entities[2], host.position)![1] = 99;

      const codec = SnapshotCodec();
      final decoded = codec.decode(
        codec.encodeDelta(capture.capture(2), baseline),
      );

      expect(decoded.isDelta, isTrue);
      expect(decoded.baselineTick, 1);
      expect(decoded.groups.first.networkIds, [
        3,
      ], reason: 'only the entity that moved should be on the wire');
    });

    test('a delta reports entities that disappeared', () {
      final entity = host.world.createEntity();
      host.world.add(entity, host.networkId, Uint64List.fromList([11]));
      host.world.add(entity, host.position);

      final baseline = capture.capture(1);
      host.world.destroyEntity(entity);

      const codec = SnapshotCodec();
      final decoded = codec.decode(
        codec.encodeDelta(capture.capture(2), baseline),
      );
      expect(decoded.despawned, [11]);
    });

    test('a foreign message is refused', () {
      expect(
        () => const SnapshotCodec().decode(Uint8List(64)),
        throwsA(isA<SnapshotFormatError>()),
      );
    });
  });

  group('a session over loopback', () {
    late Peer host;
    late Peer client;
    late LoopbackLink link;
    late NetHost netHost;
    late NetClient netClient;

    setUp(() {
      host = Peer(reverseRegistrationOrder: false);
      client = Peer(reverseRegistrationOrder: true);
      link = LoopbackLink();
      netHost = NetHost(
        world: host.world,
        set: host.set,
        networkId: host.networkId,
      );
      netClient = NetClient(
        world: client.world,
        set: client.set,
        networkId: client.networkId,
        transport: link.client,
      );
      netHost.addClient('player-1', link.host);
    });

    tearDown(() async {
      await netClient.dispose();
      await netHost.dispose();
      await link.close();
      client.dispose();
      host.dispose();
    });

    test('an entity spawned on the host appears on the client', () async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity);
      host.world.add(entity, host.position, Float32List.fromList([4, 5, 6]));

      netHost.publish();
      await settle();

      final replica = netClient.entityFor(networkId);
      expect(replica, isNotNull);
      expect(
        client.world.float32Of(replica!, client.position),
        [4, 5, 6],
        reason: 'component bytes cross unchanged, despite different local ids',
      );
    });

    test('later ticks update in place rather than respawning', () async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity);
      host.world.add(entity, host.position, Float32List.fromList([0, 0, 0]));

      netHost.publish();
      await settle();
      final replica = netClient.entityFor(networkId);

      host.world.float32Of(entity, host.position)![0] = 12;
      netHost.publish();
      await settle();

      expect(
        netClient.entityFor(networkId),
        replica,
        reason: 'the replica should be updated, not replaced',
      );
      expect(client.world.float32Of(replica!, client.position)![0], 12);
    });

    test('a component added later appears on the replica', () async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity);
      host.world.add(entity, host.position);
      netHost.publish();
      await settle();

      final replica = netClient.entityFor(networkId)!;
      expect(client.world.has(replica, client.health), isFalse);

      host.world.add(entity, host.health, Int32List.fromList([55]));
      netHost.publish();
      await settle();

      expect(client.world.has(replica, client.health), isTrue);
      expect(client.world.bytesOf(replica, client.health), isNotNull);
    });

    test('despawning on the host removes the replica', () async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity);
      host.world.add(entity, host.position);
      netHost.publish();
      await settle();
      expect(netClient.entityFor(networkId), isNotNull);

      netHost.despawn(entity);
      netHost.publish();
      await settle();

      expect(netClient.entityFor(networkId), isNull);
      expect(netClient.entityCount, 0);
    });

    test('the client acknowledges, so the host switches to deltas', () async {
      final entity = host.world.createEntity();
      netHost.spawn(entity);
      host.world.add(entity, host.position);

      netHost.publish();
      await settle();
      expect(netClient.lastAppliedTick, 1);

      // The ack has arrived, so this publish is a delta against tick 1 — and
      // with nothing changed it should carry no rows at all.
      final sent = <Uint8List>[];
      final probe = link.client.inbound.listen(sent.add);
      addTearDown(probe.cancel);

      netHost.publish();
      await settle();

      final decoded = const SnapshotCodec().decode(sent.single);
      expect(decoded.isDelta, isTrue);
      expect(decoded.baselineTick, 1);
      expect(
        decoded.groups,
        isEmpty,
        reason: 'an unchanged world should cost almost nothing to send',
      );
    });

    test('many entities replicate in one publish', () async {
      const count = 500;
      for (var i = 0; i < count; i++) {
        final entity = host.world.createEntity();
        netHost.spawn(entity);
        host.world.add(
          entity,
          host.position,
          Float32List.fromList([i * 1.0, 0, 0]),
        );
      }

      netHost.publish();
      await settle();

      expect(netClient.entityCount, count);
      final replica = netClient.entityFor(count)!;
      expect(
        client.world.float32Of(replica, client.position)![0],
        (count - 1) * 1.0,
      );
    });
  });

  _interpolationTests();

  group('ownership decides who may write what', () {
    late Peer host;
    late Peer client;
    late LoopbackLink link;
    late NetHost netHost;
    late NetClient netClient;

    setUp(() {
      // Velocity is the client's to drive; position is the authority's answer
      // about where that got them.
      host = Peer(
        reverseRegistrationOrder: false,
        includeOwner: true,
        ownerWritable: {'Velocity'},
      );
      client = Peer(
        reverseRegistrationOrder: true,
        includeOwner: true,
        ownerWritable: {'Velocity'},
      );
      link = LoopbackLink();
      netHost = NetHost(
        world: host.world,
        set: host.set,
        networkId: host.networkId,
        ownerComponent: host.owner,
      );
      netClient = NetClient(
        world: client.world,
        set: client.set,
        networkId: client.networkId,
        transport: link.client,
      );
      netHost.addClient('player-1', link.host);
    });

    tearDown(() async {
      await netClient.dispose();
      await netHost.dispose();
      await link.close();
      client.dispose();
      host.dispose();
    });

    Future<int> spawnOwned({String? owner = 'player-1'}) async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity, owner: owner);
      host.world.add(entity, host.position, Float32List.fromList([0, 0, 0]));
      host.world.add(entity, host.velocity, Float32List.fromList([0, 0, 0]));
      netHost.publish();
      await settle();
      return networkId;
    }

    test('the owner may write an owner-writable component', () async {
      final networkId = await spawnOwned();

      netClient.sendInput({
        networkId: {
          client.velocity: Float32List.fromList([3, 0, 0]),
        },
      });
      await settle();

      expect(netHost.acceptedInputs, 1);
      expect(netHost.rejections, isEmpty);

      // The authority's own world now carries what the client asked for.
      final authoritative = netHost.entityFor(networkId)!;
      expect(host.world.float32Of(authoritative, host.velocity), [3, 0, 0]);
    });

    test('a client may not write a component the authority reserves', () async {
      final networkId = await spawnOwned();

      netClient.sendInput({
        networkId: {
          client.position: Float32List.fromList([999, 0, 0]),
        },
      });
      await settle();

      expect(netHost.acceptedInputs, 0);
      expect(netHost.rejections[InputRejection.notWritable], 1);
      expect(
        host.world.float32Of(netHost.entityFor(networkId)!, host.position),
        [0, 0, 0],
        reason: 'the reserved component must be untouched',
      );
    });

    test('a client may not write an entity it does not own', () async {
      final networkId = await spawnOwned(owner: null);

      netClient.sendInput({
        networkId: {
          client.velocity: Float32List.fromList([3, 0, 0]),
        },
      });
      await settle();

      expect(netHost.acceptedInputs, 0);
      expect(netHost.rejections[InputRejection.notOwned], 1);
    });

    test('an unknown entity is refused rather than crashing', () async {
      await spawnOwned();
      netClient.sendInput({
        9999: {
          client.velocity: Float32List.fromList([1, 0, 0]),
        },
      });
      await settle();
      expect(netHost.rejections[InputRejection.unknownEntity], 1);
    });

    test('a malformed payload is counted, not thrown', () async {
      final networkId = await spawnOwned();
      // A row claiming velocity but carrying too few bytes.
      final bit = host.set.bitOf(host.velocity)!;
      link.client.send(
        encodeInput(
          InputMessage(
            tick: 0,
            entries: [
              InputEntry(
                networkId: networkId,
                mask: 1 << bit,
                row: Uint8List(4),
              ),
            ],
          ),
        ),
      );
      await settle();

      expect(netHost.rejections[InputRejection.malformed], 1);
      expect(netHost.acceptedInputs, 0);
    });

    test('ownership replicates so a client knows what it drives', () async {
      final networkId = await spawnOwned();
      final replica = netClient.entityFor(networkId)!;
      final ownerBytes = client.world.bytesOf(replica, client.owner)!;
      expect(
        ByteData.sublistView(ownerBytes).getUint32(0, Endian.little),
        1,
        reason: 'the first client is index 1; 0 means the authority',
      );
    });

    test('a departing client loses what it owned', () async {
      final networkId = await spawnOwned();
      expect(netHost.ownerOf(networkId), 'player-1');

      await netHost.removeClient('player-1');
      expect(
        netHost.ownerOf(networkId),
        isNull,
        reason: 'ownership must not survive under a reconnectable name',
      );
    });

    test('writing an unreplicated component is a programming error', () async {
      final networkId = await spawnOwned();
      final stray = client.world.registerComponent(
        'Stray',
        kind: ComponentKind.int32,
      );
      expect(
        () => netClient.sendInput({
          networkId: {
            stray: Int32List.fromList([1]),
          },
        }),
        throwsArgumentError,
      );
    });
  });
}

void _interpolationTests() {
  group('interpolation', () {
    late Peer host;
    late Peer client;
    late LoopbackLink link;
    late NetHost netHost;
    late NetClient netClient;
    late double now;

    setUp(() {
      now = 0;
      host = Peer(reverseRegistrationOrder: false);
      client = Peer(reverseRegistrationOrder: true);
      link = LoopbackLink();
      netHost = NetHost(
        world: host.world,
        set: host.set,
        networkId: host.networkId,
      );
      netClient = NetClient(
        world: client.world,
        set: client.set,
        networkId: client.networkId,
        transport: link.client,
        interpolationDelay: const Duration(milliseconds: 100),
        clock: () => now,
      );
      netHost.addClient('player-1', link.host);
    });

    tearDown(() async {
      await netClient.dispose();
      await netHost.dispose();
      await link.close();
      client.dispose();
      host.dispose();
    });

    test('nothing is written until the world is advanced', () async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity);
      host.world.add(entity, host.position, Float32List.fromList([10, 0, 0]));
      netHost.publish();
      await settle();

      expect(netClient.isInterpolating, isTrue);
      expect(
        netClient.entityFor(networkId),
        isNull,
        reason:
            'an interpolating client shows the past, not the newest '
            'message the moment it lands',
      );

      netClient.advance();
      expect(netClient.entityFor(networkId), isNotNull);
    });

    test('a float component blends between two states', () async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity);
      host.world.add(entity, host.position, Float32List.fromList([0, 0, 0]));

      netHost.publish();
      await settle();

      now = 0.1;
      host.world.float32Of(entity, host.position)![0] = 10;
      netHost.publish();
      await settle();

      // Render time is now 0.2 - 0.1 = 0.1, exactly the second state.
      now = 0.2;
      netClient.advance();
      final replica = netClient.entityFor(networkId)!;
      expect(client.world.float32Of(replica, client.position)![0], 10);

      // Halfway between the two arrivals: halfway between the two positions.
      now = 0.15;
      netClient.advance();
      expect(
        client.world.float32Of(replica, client.position)![0],
        closeTo(5, 1e-4),
      );
    });

    test(
      'a non-float component takes the earlier value, never a blend',
      () async {
        final entity = host.world.createEntity();
        final networkId = netHost.spawn(entity);
        host.world.add(entity, host.position, Float32List.fromList([0, 0, 0]));
        host.world.add(entity, host.health, Int32List.fromList([100]));
        netHost.publish();
        await settle();

        now = 0.1;
        ByteData.sublistView(
          host.world.bytesOf(entity, host.health)!,
        ).setInt32(0, 50, Endian.little);
        netHost.publish();
        await settle();

        now = 0.15;
        netClient.advance();
        final replica = netClient.entityFor(networkId)!;
        final health = ByteData.sublistView(
          client.world.bytesOf(replica, client.health)!,
        ).getInt32(0, Endian.little);
        expect(
          health,
          100,
          reason: 'half of a hundred and fifty is not a health value',
        );
      },
    );

    test('past the newest state it holds rather than extrapolating', () async {
      final entity = host.world.createEntity();
      final networkId = netHost.spawn(entity);
      host.world.add(entity, host.position, Float32List.fromList([7, 0, 0]));
      netHost.publish();
      await settle();

      now = 10;
      netClient.advance();
      final replica = netClient.entityFor(networkId)!;
      expect(
        client.world.float32Of(replica, client.position)![0],
        7,
        reason:
            'the authority has gone quiet; inventing motion would be a '
            'guess presented as fact',
      );
    });
  });
}
