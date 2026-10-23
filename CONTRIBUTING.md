# Development Guide

## Building the development environment

```bash
$ git clone https://github.com/osrg/gobgp.git
$ cd gobgp && go mod download
```

Now ready to modify the code and build two binaries, `cmd/gobgp` and `cmd/gobgpd`. For maximum performance on modern AMD64 processors, set `GOAMD64=v3` before running `go build` (see [Go minimum requirements](https://go.dev/wiki/MinimumRequirements#architectures)).

## Design Considerations

### OpenConfig, gRPC, and native APIs

In GoBGP, multiple structures are defined to represent the same concept, in order to support multiple APIs. For example, to represent a peer, there are three different structures: `config.Neighbor`, `api.Peer`, and `apiutil.Peer`.

`config.Neighbor` is the structure used by OpenConfig and is generated from YANG files. OpenConfig is a standard that aims to make the configuration and management of vendor-neutral network devices programmable. GoBGP uses OpenConfig as its configuration file API. Since OpenConfig defines data structures in a generic, language-agnostic way, the most suitable Go data types are not always used. Although it is possible to exert some control over how certain types are mapped to Go, YANG can define constructs such as union types that Go cannot natively represent. In addition, the data layout is designed to be human-readable, which may not always be optimal for the computer in terms of concurrency and locking.

`api.Peer` is the structure used for gRPC and is generated from .proto files. gRPC is the primary API of GoBGP; all control operations can be performed through gRPC. The CLI is also implemented using gRPC. Similar to OpenConfig, gRPC defines data structures with generic, language-independent types to ensure support across different programming languages. Unlike OpenConfig, however, there is no way to customize how these types are converted into Go types, which makes it even less convenient. As a result, the most optimal Go types are not always used. For example, a peer’s IP address is represented as a string rather than the more suitable netip.Addr type.

`apiutil.Peer` is what we call the “native API.” While gRPC APIs have the advantage of supporting multiple programming languages, they also come with significant overhead. The native API, on the other hand, is a lightweight API designed specifically for Go. It defines data structures using the most appropriate Go types, such as netip.Addr, to achieve better efficiency and usability.

Finally, although it is not exposed as a public API, the `server.peer` structure also represents a peer. This structure is used internally by GoBGP to manage peers. In order to avoid the overhead of defining a new structure from scratch, we reused the OpenConfig structure within this internal component. However, this turned out to be a major design mistake. As described above, OpenConfig structures do not necessarily use the most suitable Go data types.
For example, GoBGP currently uses strings to represent peer addresses in many internal places. This is a direct consequence of relying on OpenConfig structures internally. Over time, these structures should be gradually replaced with more appropriate native Go types.
Therefore, when introducing new internal structures, we should avoid using OpenConfig types and instead define new data structures that are optimized for Go.

### Memory Usage

If your new feature requires adding members to existing data structures or creating new structures that will increase memory usage, please consider how memory consumption will change in relation to increases in both route count and peer count.

For instance, imagine an environment where 10 peers are connected, each carrying full routes. For the sake of simplicity in calculations, let's assume full routes consist of 1 million routes.

If you add an 8-byte member to the Path structure, since the number of Path structures increases proportionally to both the number of routes and the number of peers, this would result in approximately 80MB increase in memory usage."

If you add an 8-byte member to the Destination struture, since the number of Destiation structures increases proportionally to the number of routes, this would result in approximately 8MB increase in memory usage.

If you add an 8-byte member to the Peer struture, since the number of Peer structures increases proportionally to the number of peers, this would result in approximately 80 bytes increase in memory usage.

Please be cautious about changes that increase memory usage proportionally to the number of routes (and be even more cautious about changes that are proportional to both the number of routes and the number of peers). The benefits must justify such increases (for example, huge performance improvement).

### Locking

GoBGP uses a two-level mutex design to manage concurrent access to shared state. The server-level mutex (`sharedData.mu`) protects global state such as the RIB tables and peer map. All management operations (API calls) and FSM event processing which access to those data are serialized through this mutex. The per-peer mutex (`peer.fsm.lock`) protects individual peer state including configuration and capability negotiation, using a read-write lock to allow concurrent reads while maintaining exclusive writes.

To prevent deadlocks, always follow this lock ordering rule: **acquire the server-level mutex first, then acquire peer-level locks if needed**. Never acquire the server-level mutex while holding a peer-level lock. When modifying code that involves locking, ensure critical sections are kept as short as possible, and consider copying data before releasing locks to minimize contention. The server-level mutex is the primary scalability bottleneck, so any changes affecting lock duration or frequency should be carefully evaluated for performance impact, especially in environments with many peers or large routing tables.

## Getting your code into GoBGP

### Creating a commit

Separate each **logical change** into a separate commit.

There is no strict size limit for a single commit. This is because a simple large commit that just moves packages cannot be compared in terms of potential risk with a commit that modifies fundamental BGP functionality. However, if you're changing fundamental functionality, keep in mind that the larger the commit, the less likely it is to be merged.

A Git commit message should include enough information to explain why the change is necessary, in order to convince the maintainer to merge it.

### Submitting a pull request

Every commit that is merged must pass continuous integration (CI) checks. For this reason, each pull request should ideally contain only one commit. This is because if you push multiple commits at once and create a pull request, some of the intermediate commits may not be tested by CI.

There are exceptions where a pull request may include multiple commits, especially if the risk of breaking the code is considered low. For example, one commit may add a new feature, and a second commit may add unit tests or benchmarks for that feature. Another example is when adding a serializer and decoder for a new address family—such commits may also be treated as exceptions, since they are unlikely to affect existing GoBGP users. However, this does not apply to changes that affect fundamental parts of the code used by all users.

Please keep in mind that each pull request should represent a single logical change. If a commit has a valid standalone reason to be merged into GoBGP, it should be treated as a single logical change and submitted in its own pull request, rather than being grouped together with other commits.

If you're working on a large change, it may sometimes be unavoidable to include multiple commits in a single pull request. However, if you're modifying code in fundamental parts of the BGP implementation, please try to keep the number of commits to four or fewer, excluding tests. If the pull request exceeds that, the likelihood of it being reviewed and merged becomes significantly lower. So please take the time to split your work into as many logical and self-contained pieces as possible.

### Responding to review comments

Please treat your pull request as "frozen" once it has been created. Adding new commits or modifying the code unprompted makes review much harder. Only changes based on review comments should be made.

During the review process, you may discover that changes you didn’t originally anticipate are required. For example, modifying code X in response to review comments may reveal the need to adjust code B as well. Even in such cases, if the change to code B can be considered a separate logical change, it is expected that you do not add a new commit to the existing pull request. Instead, you should create a new pull request for the code B change and get it merged first.

## Using AI tools

You may use AI tools such as large language models. These rules follow the Linux kernel guidelines for tool-generated content (`Documentation/process/generated-content.rst`).

- Say that you used one. Add an `Assisted-by: Claude Opus 5 <noreply@anthropic.com>` trailer to the commit message. Do not use `Co-Authored-By`. A tool is not an author.
- You are responsible for the result. You must understand the whole change and be able to explain it. If you cannot answer a review comment without going back to the tool, do not send the change. A pull request that its author cannot explain may be closed without a review.
- The same applies to an issue. Do not open one only because a tool read the code and said that it looks wrong. Reproduce the problem first, and send a fix if you can write one.

Trivial help, such as completion or formatting, needs no disclosure.

## Testing

Before you create a pull request, run CI in your own fork of GoBGP on GitHub and make sure that all the tests pass. This includes both unit and integration tests.

CI is defined in [ci.yml](https://github.com/osrg/gobgp/blob/master/.github/workflows/ci.yml). It runs on every push, so you do not need Docker on your machine to run it:

1. Fork [osrg/gobgp](https://github.com/osrg/gobgp) on GitHub.
2. Open the Actions tab of your fork and enable workflows. GitHub does not run the workflows of a fork until you do this.
3. Push your branch to your fork. Then check the result of the `ci` workflow in the Actions tab.

If you need to debug the integration tests, it's a good idea to run them [locally](https://github.com/osrg/gobgp/blob/master/test/scenario_test/README.md).

## Changing the gRPC API

To generate gRPC code from Protobuf definitions, first make sure you have [buf](https://github.com/bufbuild/buf) installed.

Then run the following:

```bash
$ cd proto
$ buf generate
```

## Fork releases and identity

This repository releases **n-gobgp** independently from osrg/gobgp. The initial
fork version is `0.1.0-dev`; release tags use `v0.1.0`, `v0.1.1`, etc. A release
is driven by readiness and validation, not the upstream monthly schedule. The
Go module path, binary names and protobuf API remain compatible with GoBGP v4.
The fork version does not describe an upstream API version.

`internal/pkg/version/version.go` is the source of truth for the fork version
and the incorporated upstream base. `UpstreamCommit` is the full commit from
osrg/gobgp that was actually merged, not the latest upstream HEAD. Update the
upstream version and commit together when incorporating a new base. For a
selective cherry-pick, keep that base and record the selected SHA and validation
in the PR. Every upstream intake must describe its purpose, risk and tests.

Both binaries expose fork version, fork commit, working-tree state, upstream
repository, upstream version and upstream commit through `--version`. Builds
without VCS metadata report `unknown` rather than inventing a commit. The
`COMMIT`, `IDENTIFIER`, `METADATA` and `DIRTY` linker variables allow explicit
build metadata; direct `go build` uses Go VCS information when available.

Use Go **1.27.3** and run `tools/build.sh` to build both binaries in `dist/local`.
It uses the full fork SHA, no build timestamp, disabled CGO, trimmed paths and
an empty build ID. `tools/check-build.sh` builds twice and compares the actual
binary bytes, checks CLI identity and tests rejection of a mismatched release
tag in a clean checkout. Reproduction assumes the same source, dependencies,
Go version, target architecture and build flags.

To release, update the fork MAJOR/MINOR/PATCH constants, merge through a PR,
wait for the main commit's `required` check, and create its matching `vX.Y.Z`
tag. Do not reuse an existing release tag. The release workflow requires main
ancestry and a successful check from GitHub Actions, then uses pinned GoReleaser
**v2.19.0**. Release builds remove `dev` and include the full fork commit. Tag
mismatch or a dirty checkout fails before publication. Assets include both
binaries, source and checksums. No release tag is created by the build scripts.

## Required CI

The `required` job aggregates every CI job, including the AS 0/BFD regressions,
race detection, 32-bit tests, Kafka with real brokers, generated API/YANG files,
platform builds, release identity and routing scenarios. A failed, skipped,
cancelled or missing dependency fails the gate. Adding a job without adding it
to the gate's `needs` also fails. Pushes to main and PRs run CI; a feature branch
does not run a duplicate push pipeline. New commits cancel an older PR run.

`.github/main-protection.json` declares main's required check (`required`, from
GitHub Actions), up-to-date branches and enforcement for administrators. PRs
are required, with zero mandatory external approvals so the maintainer can
merge their own work after CI. Force pushes and deletion are disabled. An
administrator can apply or reconcile it with:

```bash
gh api --method PUT repos/nishisan-dev/n-gobgp/branches/main/protection \
  --input .github/main-protection.json
```

Changing the YAML alone does not configure GitHub branch protection. Check the
server-side policy after applying it; never silently substitute a bypass when
repository permissions or plan limitations reject the change.

## Layout

The GoBGP project adopts [Standard Go Project Layout](https://github.com/golang-standards/project-layout).

## Fuzzing

Run [Go Fuzzing](https://go.dev/security/fuzz)

```bash
go test -fuzz=FuzzParseRTR                      $PWD/pkg/packet/rtr
go test -fuzz=FuzzParseBMPMessage               $PWD/pkg/packet/bmp
go test -fuzz=FuzzParseBGPMessage               $PWD/pkg/packet/bgp
go test -fuzz=FuzzParseLargeCommunity           $PWD/pkg/packet/bgp
go test -fuzz=FuzzParseFlowSpecComponents       $PWD/pkg/packet/bgp
go test -fuzz=FuzzMRT                           $PWD/pkg/packet/mrt
go test -fuzz=FuzzZapi                          $PWD/internal/pkg/zebra
```
