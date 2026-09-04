import 'package:orbis_core/orbis_core.dart';

/// A component that crosses the wire, and the rules for who may write it.
class ReplicatedComponent {
  const ReplicatedComponent({
    required this.type,
    required this.bit,
    this.ownerWritable = false,
  });

  final ComponentType type;

  /// This component's position in the set, and its bit in a group's mask.
  final int bit;

  /// Whether the client that owns an entity may write this component and have
  /// the authority accept it.
  ///
  /// Ownership is a security surface rather than bookkeeping: everything not
  /// marked here is the authority's alone, and a client that sends it anyway
  /// is ignored rather than trusted.
  final bool ownerWritable;

  @override
  String toString() =>
      'ReplicatedComponent(${type.name}, bit $bit'
      '${ownerWritable ? ', owner-writable' : ''})';
}

/// The components that replicate, in an order both ends agree on.
///
/// The order is derived from the component names rather than from registration
/// order, so a host and a client that declare the same components arrive at the
/// same layout without exchanging a schema. Registration order would make the
/// wire format depend on which package happened to load first.
class ReplicationSet {
  ReplicationSet._(this._components, this._bitByComponentId);

  /// Builds a set from [types], with [ownerWritable] naming the ones an owning
  /// client is allowed to write.
  factory ReplicationSet(
    List<ComponentType> types, {
    Set<String> ownerWritable = const {},
  }) {
    if (types.length > 64) {
      // The group mask is a single 64-bit word, which keeps a group header to
      // twelve bytes. More than sixty-four replicated component types is a
      // different design, not a bigger number.
      throw ArgumentError('A replication set holds at most 64 components.');
    }

    final sorted = [...types]..sort((a, b) => a.name.compareTo(b.name));
    final names = <String>{};
    final components = <ReplicatedComponent>[];
    final bits = <int, int>{};

    for (var bit = 0; bit < sorted.length; bit++) {
      final type = sorted[bit];
      if (!names.add(type.name)) {
        throw ArgumentError('Component "${type.name}" is listed twice.');
      }
      components.add(
        ReplicatedComponent(
          type: type,
          bit: bit,
          ownerWritable: ownerWritable.contains(type.name),
        ),
      );
      bits[type.id] = bit;
    }

    return ReplicationSet._(List.unmodifiable(components), bits);
  }

  final List<ReplicatedComponent> _components;
  final Map<int, int> _bitByComponentId;

  List<ReplicatedComponent> get components => _components;

  int get length => _components.length;

  /// The bit for a component, or null if it does not replicate.
  int? bitOf(ComponentType type) => _bitByComponentId[type.id];

  int? bitOfId(int componentId) => _bitByComponentId[componentId];

  ReplicatedComponent atBit(int bit) => _components[bit];

  /// The mask for a set of component ids, ignoring any that do not replicate.
  int maskOfIds(Iterable<int> componentIds) {
    var mask = 0;
    for (final id in componentIds) {
      final bit = _bitByComponentId[id];
      if (bit != null) mask |= 1 << bit;
    }
    return mask;
  }

  /// The bits set in [mask], ascending — the order a row's components are
  /// written in.
  Iterable<int> bitsOf(int mask) sync* {
    for (var bit = 0; bit < _components.length; bit++) {
      if (mask & (1 << bit) != 0) yield bit;
    }
  }

  /// How many bytes one entity occupies under [mask].
  int strideOf(int mask) {
    var stride = 0;
    for (final bit in bitsOf(mask)) {
      stride += _components[bit].type.byteSize;
    }
    return stride;
  }
}
