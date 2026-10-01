# One manual test-fixture compatibility change is required

This published branch intentionally leaves `codex-rs/core/tests/suite/compact_remote.rs` unchanged. Automated publication of its full contents was blocked because the existing public file contains an authentication-shaped test fixture. No real credential was introduced by this feature.

The complete local patch was validated, but this public branch is **not complete for the broader core integration-test build** until the following one-line compatibility change is applied. The native-contract CI workflow reports that prerequisite instead of claiming a complete green build.

From a checkout of this branch, inspect and apply the credential-free patch:

```sh
git apply --check external-orchestrator-bridge/manual-test-compat.patch
git apply external-orchestrator-bridge/manual-test-compat.patch
```

The only change adds `incarnation_id: None,` to the `RealtimeConversationRealtimeEvent` pattern in `start_realtime_conversation`, immediately before `payload:`. It does not change the existing fixture value or authentication behavior.

After reviewing the one-line diff, commit and push it using your own GitHub tools. No merge, deployment or installed application replacement is needed. The provided CI then validates the exact resulting branch revision. Do not bypass sandbox or network guards to make tests appear to pass.

All public-side runtime changes, generated schemas, bridge files and other tests are included in the branch. Real closed-source desktop/audio integration remains separately unverified.
