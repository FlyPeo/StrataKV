# release-packaging Delta Spec

## Purpose

Define the self-contained binary release package for StrataKV internal use: one command produces a versioned archive that starts a single-machine multi-process cluster and exposes the C++ SDK without requiring the source tree or hand-installed third-party libraries on the target machine.

## ADDED Requirements

### Requirement: Release archive artifact

The packaging command SHALL build the Release binaries and produce an archive named `stratakv-<version>-linux-x64.tar.gz` whose top-level directory is `stratakv-<version>-linux-x64/`. The package directory SHALL contain the server executables (`stratakv-node`, `stratakv-tso`, `stratakv-meta`, `stratakv-admin`), the deployment scripts (`stratakv-server`, `stratakvctl`), the SDK static libraries and public headers, a CMake package configuration, runtime config templates, an SDK usage example, a quickstart document, and a file recording the package version. The version string SHALL be identical in the archive name and in the recorded version file.

#### Scenario: Successful packaging run

- **WHEN** the packaging command runs on a clean working tree with the toolchain available
- **THEN** the archive is produced with the versioned top-level directory and all listed content groups present

#### Scenario: Version consistency

- **WHEN** the archive is unpacked
- **THEN** the version recorded inside the package matches the version embedded in the archive and directory names

### Requirement: Self-contained runtime dependencies

Every executable shipped in the package SHALL resolve all non-base third-party shared libraries (including Boost.Context, Boost.Serialization, protobuf, RocksDB, and their transitive dependencies such as snappy, gflags, lz4, zstd) from the package's own `lib/` directory, and MUST NOT require those libraries to be installed system-wide on the target machine. The packaging process SHALL determine the exact set of bundled libraries from the built binaries rather than a hand-maintained list, and SHALL abort with an actionable error listing the missing artifact if any required shared library cannot be collected.

#### Scenario: Run on a machine without manual library installation

- **WHEN** the package is unpacked on a glibc-compatible x86_64 Linux machine that has no hand-installed RocksDB or Boost runtime libraries
- **THEN** every packaged executable starts and its dynamic dependencies resolve to files inside the package

#### Scenario: Bundling failure fails loudly

- **WHEN** a shared library required by a packaged executable cannot be located during packaging
- **THEN** packaging exits nonzero, reports the missing library and the binary that needs it, and produces no archive

### Requirement: SDK consumption from the release package

The package SHALL allow a client program located outside the package to compile and link against the SDK using the CMake package configuration alone. Linking via the exported target SHALL pull in all required StrataKV static libraries, include directories, and system/third-party link dependencies without the consumer manually enumerating them.

#### Scenario: Example client builds and runs against the package

- **WHEN** the shipped example is built from a directory outside the package with the package's CMake package configuration on the search path
- **THEN** it compiles, links, and successfully performs a put and a get against a running packaged cluster

#### Scenario: Consumer does not enumerate dependencies

- **WHEN** the example's CMakeLists references only the exported SDK target
- **THEN** the build succeeds without listing StrataKV core/RPC libraries or third-party link flags itself

### Requirement: Build-free operation in release packages

Running the deployment script from inside a release package SHALL NOT attempt any CMake configure or build step and SHALL start the cluster using the packaged binaries. Source-tree deployments SHALL retain the current behavior, including automatic building. The release-package path SHALL NOT require the source repository to exist on the target machine.

#### Scenario: Cluster start inside a package without a source tree

- **WHEN** the deployment script is invoked with `up` from an unpacked package on a machine that has no StrataKV source checkout
- **THEN** the cluster starts from packaged binaries without any build attempt, and `status`/`verify` report the expected running processes and leaders

#### Scenario: Source-tree behavior is preserved

- **WHEN** the deployment script is invoked with `up` from a source checkout
- **THEN** it builds and starts as before, with no behavior change required by this capability

### Requirement: Release acceptance gate

Packaging SHALL include a release acceptance pass that unpacks the produced archive into a clean temporary directory, starts a cluster with the packaged deployment script, waits for cluster health, performs a write and a read through the shipped command-line client, and stops the cluster. The packaging command SHALL exit nonzero if any acceptance step fails, and a package SHALL be considered releasable only after this pass succeeds.

#### Scenario: Acceptance pass succeeds

- **WHEN** the full acceptance flow completes on the unpacked archive
- **THEN** verification reports a healthy cluster, the written key is read back with the expected value, and packaging reports the archive as releasable

#### Scenario: Acceptance failure blocks release

- **WHEN** any acceptance step fails (cluster does not become healthy, or the write/read round-trip mismatches)
- **THEN** packaging exits nonzero, retains the failure logs for inspection, and does not report the archive as releasable

### Requirement: Package cleanup and idempotence

Re-running the packaging command SHALL produce a fresh archive without being corrupted by leftovers from a previous run, and SHALL stop any acceptance cluster it started before exiting, whether it succeeds or fails.

#### Scenario: Repeated packaging runs

- **WHEN** packaging runs twice in a row in the same workspace
- **THEN** the second run yields a valid archive and no acceptance processes or temporary projects remain afterwards
