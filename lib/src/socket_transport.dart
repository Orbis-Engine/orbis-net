import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'transport.dart';

/// A transport over a WebSocket.
///
/// WebSockets rather than raw UDP, for now, and the reason is framing: a
/// snapshot is a message with a length, and WebSocket already delivers whole
/// messages in order. Building sequencing, acknowledgement and reassembly on
/// top of datagrams is a real piece of work, and it is not the piece that
/// makes Orbis worth using. When the frame budget says otherwise, this
/// interface is where a datagram transport goes.
class SocketTransport implements Transport {
  SocketTransport(this._socket) {
    _subscription = _socket.listen(
      (message) {
        // A text frame is not something replication sends, so it is somebody
        // else's protocol on our port. Dropping it is kinder than crashing.
        if (message is List<int>) {
          _inbound.add(Uint8List.fromList(message));
        }
      },
      onError: (Object error, StackTrace stack) =>
          _inbound.addError(error, stack),
      onDone: () {
        _closed = true;
        if (!_inbound.isClosed) _inbound.close();
      },
      cancelOnError: false,
    );
  }

  /// Connects to a host.
  static Future<SocketTransport> connect(
    Uri url, {
    Duration timeout = const Duration(seconds: 10),
  }) async {
    final socket = await WebSocket.connect(url.toString()).timeout(timeout);
    return SocketTransport(socket);
  }

  final WebSocket _socket;
  final StreamController<Uint8List> _inbound =
      StreamController<Uint8List>.broadcast();
  late final StreamSubscription<dynamic> _subscription;
  bool _closed = false;

  bool get isOpen => !_closed && _socket.readyState == WebSocket.open;

  /// Completes when the peer goes away.
  Future<void> get done => _socket.done;

  @override
  Stream<Uint8List> get inbound => _inbound.stream;

  @override
  void send(Uint8List message) {
    if (!isOpen) return;
    _socket.add(message);
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    await _subscription.cancel();
    if (!_inbound.isClosed) await _inbound.close();
    await _socket.close();
  }
}

/// Accepts connections on behalf of an authority.
///
/// Deliberately thin: it hands out transports and says nothing about who is
/// allowed to connect. Authentication belongs to the game, which knows what an
/// account is; the engine only knows what a socket is.
class SocketServer {
  SocketServer._(this._server, this._connections);

  /// Listens on [address] and [port].
  ///
  /// Pass port zero to let the operating system choose one, which is what
  /// tests should do rather than picking a number and hoping.
  static Future<SocketServer> bind({
    Object address = '127.0.0.1',
    int port = 0,
  }) async {
    final server = await HttpServer.bind(address, port);
    final connections = StreamController<SocketTransport>.broadcast();

    server.listen(
      (request) async {
        if (!WebSocketTransformer.isUpgradeRequest(request)) {
          // Something that is not a client asking to play. Answered rather
          // than ignored, so a browser pointed here sees why nothing happens.
          request.response
            ..statusCode = HttpStatus.badRequest
            ..write('This port speaks the Orbis session protocol.');
          await request.response.close();
          return;
        }
        final socket = await WebSocketTransformer.upgrade(request);
        if (!connections.isClosed) {
          connections.add(SocketTransport(socket));
        }
      },
      onError: connections.addError,
      cancelOnError: false,
    );

    return SocketServer._(server, connections);
  }

  final HttpServer _server;
  final StreamController<SocketTransport> _connections;

  /// The port actually bound, which matters when zero was requested.
  int get port => _server.port;

  Uri get url => Uri.parse('ws://${_server.address.host}:${_server.port}');

  /// Each client as it arrives. The caller gives it to the host.
  Stream<SocketTransport> get connections => _connections.stream;

  Future<void> close() async {
    await _connections.close();
    await _server.close(force: true);
  }
}
