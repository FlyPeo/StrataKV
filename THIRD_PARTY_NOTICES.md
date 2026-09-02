# Third-Party Provenance Notes

StrataKV uses the third-party libraries listed in the root `README.md`, including
Boost.Context, Protobuf, RocksDB and Muduo. Their licenses are distributed by
their respective upstream projects and system packages.

The repository also contains code with an educational/open-source lineage that
must not be represented as a from-scratch implementation:

- the Raft and RPC foundations were developed from public teaching-project
  material, including an MIT 6.824-style Raft lab structure and a
  Protobuf/Muduo RPC teaching implementation;
- the Pulsar runtime historically contained identifiers inherited from the
  Sylar coroutine project;
- the original source headers credited `swx` on the configuration, Raft RPC,
  Region peer and persistence files.

The exact upstream repositories, baseline revisions and license texts still
need to be recorded before redistributing derived source. Until that review is
complete, contributors must describe their work as modifications and additions
on top of those foundations, not as an entirely original implementation.

