# Test-fixture compatibility change

**Resolved on this branch:** the repository owner applied the change in commit `4d317944c27819703fbcffd1e465df1ffee70bde`. Do not apply the patch again to current checkouts. The instructions below are retained for older snapshots; CI checks the prerequisite before compiling the native tests.

The initial published snapshot intentionally left `codex-rs/core/tests/suite/compact_remote.rs` unchanged. Automated publication of its full contents was blocked because the existing public file contains an authentication-shaped test fixture. No real credential was introduced by this feature.

The complete local patch was validated, but that initial snapshot was **not complete for the broader core integration-test build** until the following one-line compatibility change was applied. The native-contract CI workflow reports that prerequisite instead of claiming a complete green build.

For an older checkout missing this change, inspect and apply the credential-free patch:

```sh
git apply --check external-orchestrator-bridge/manual-test-compat.patch
git apply external-orchestrator-bridge/manual-test-compat.patch
```

The only change adds `incarnation_id: None,` to the `RealtimeConversationRealtimeEvent` pattern in `start_realtime_conversation`, immediately before `payload:`. It does not change the existing fixture value or authentication behavior.

After reviewing the one-line diff, commit and push it using your own GitHub tools. No merge, deployment or installed application replacement is needed. The provided CI then validates the exact resulting branch revision. Do not bypass sandbox or network guards to make tests appear to pass.

All public-side runtime changes, generated schemas, bridge files and other tests are included in the branch. Real closed-source desktop/audio integration remains separately unverified.
