# orbis-net

Multiplayer for [Orbis](https://github.com/Orbis-Engine/orbis).

Replication reads component *columns* straight out of the engine rather than
walking entities asking each what changed, so capturing five hundred transforms
is a handful of bulk copies rather than five hundred lookups. That is the
engine's storage decision paying for itself a second time.

```sh
git clone https://github.com/Orbis-Engine/orbis.git       # beside this one
./tool/link_local.sh                                   # point at that checkout
./tool/check.sh
```

## What it does

- **Snapshots and deltas.** Deltas are built against the tick a client
  acknowledged, not the last thing sent, so a dropped message costs one larger
  snapshot rather than a world that quietly diverges.
- **Ownership.** A client proposes; the authority decides. Only components
  declared owner-writable, only on entities it agrees that client owns. Every
  refusal is counted rather than thrown.
- **Interpolation.** Renders a little behind so remote entities move rather
  than step. Floats blend; integers take the earlier value.
- **Transports.** A loopback link, so a single-player build runs the real
  networked path, and a socket transport for actual play.

## Licence

MIT, © 2026 Chris Beckett. Nothing third-party ships inside this one — see
[LICENSE](LICENSE).
