# release-packaging Delta Spec

## ADDED Requirements

### Requirement: Server and client separation in the package

The release package SHALL separate server-side and client-side content into distinct top-level directories: `server/` SHALL hold the server executables, the deployment scripts, the runtime data root, and the config templates; `client/` SHALL hold the client tool executable and the SDK usage example. Shared SDK content (static libraries, public headers, CMake package configuration) and bundled third-party libraries SHALL remain at the package root. A reader SHALL be able to identify from the directory layout alone which executables start servers and which access the cluster as a client.

#### Scenario: Directory layout distinguishes roles

- **WHEN** the archive is unpacked and its top two levels are listed
- **THEN** server executables and cluster scripts appear only under `server/`, and the client tool appears only under `client/`

#### Scenario: Shared content stays at the root

- **WHEN** a client program consumes the package via `CMAKE_PREFIX_PATH` pointing at the package root
- **THEN** the SDK libraries, headers, and CMake package configuration resolve from the package root as before

## MODIFIED Requirements

### Requirement: Release archive artifact

The packaging command SHALL build the Release binaries and produce an archive named `stratakv-<version>-linux-x64.tar.gz` whose top-level directory is `stratakv-<version>-linux-x64/`. The package SHALL contain: under `server/`, the server executables (`stratakv-node`, `stratakv-tso`, `stratakv-meta`, `stratakv-admin`) and the deployment scripts (`stratakv-server`, `stratakvctl`); under `client/`, the client tool executable (`stratakv-client`) and an SDK usage example; at the root, the SDK static libraries and public headers, a CMake package configuration, a quickstart document, and a file recording the package version. The version string SHALL be identical in the archive name and in the recorded version file.

#### Scenario: Successful packaging run

- **WHEN** the packaging command runs on a clean working tree with the toolchain available
- **THEN** the archive is produced with the versioned top-level directory and all listed content groups present in their designated directories

#### Scenario: Version consistency

- **WHEN** the archive is unpacked
- **THEN** the version recorded inside the package matches the version embedded in the archive and directory names

### Requirement: Build-free operation in release packages

Running the deployment script from inside a release package SHALL NOT attempt any CMake configure or build step and SHALL start the cluster using the packaged binaries from the package's `server/bin/` directory, with runtime data under `server/runtime/<project>/`. Source-tree deployments SHALL retain the current behavior, including automatic building. The release-package path SHALL NOT require the source repository to exist on the target machine.

#### Scenario: Cluster start inside a package without a source tree

- **WHEN** the deployment script is invoked with `up` from `server/scripts/` of an unpacked package on a machine that has no StrataKV source checkout
- **THEN** the cluster starts from packaged binaries without any build attempt, and `status`/`verify` report the expected running processes and leaders

#### Scenario: Source-tree behavior is preserved

- **WHEN** the deployment script is invoked with `up` from a source checkout
- **THEN** it builds and starts as before, with no behavior change required by this capability

### Requirement: Release acceptance gate

Packaging SHALL include a release acceptance pass that unpacks the produced archive into a clean temporary directory, starts a cluster with the packaged deployment script, waits for cluster health, performs a write and a read round-trip with the packaged client tool, verifies the client tool's list output includes the written key, and stops the cluster. The packaging command SHALL exit nonzero if any acceptance step fails, and a package SHALL be considered releasable only after this pass succeeds.

#### Scenario: Acceptance pass succeeds

- **WHEN** the full acceptance flow completes on the unpacked archive
- **THEN** verification reports a healthy cluster, the client tool's write/read round-trip matches, the list output contains the written key, and packaging reports the archive as releasable

#### Scenario: Acceptance failure blocks release

- **WHEN** any acceptance step fails (cluster does not become healthy, the write/read round-trip mismatches, or the list output omits the written key)
- **THEN** packaging exits nonzero, retains the failure logs for inspection, and does not report the archive as releasable
