# Security Policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, report them privately through one of:

- GitHub's [private vulnerability reporting](https://github.com/oyro-os/pylon/security/advisories/new)
  ("Report a vulnerability" under the repository's **Security** tab), or
- email **smrockypk@gmail.com** with the details.

Please include enough information to reproduce the issue — affected version/commit, configuration,
and a minimal proof of concept where possible. We will acknowledge your report, investigate, and
keep you updated on the fix and disclosure timeline.

## Supported versions

Pylon is pre-1.0 and under active development. Security fixes are applied to the latest `master`.
Until a 1.0 release, only the most recent commit on `master` is supported.

## Scope

Pylon is a network-facing realtime server. Reports of particular interest include: authentication
or signature-verification bypasses (subscription auth, user signin, REST HTTP auth, webhook
signatures), remote crashes / panics reachable from untrusted client frames or REST requests,
resource-exhaustion vectors that bypass the overload controls, and TLS/transport flaws.
