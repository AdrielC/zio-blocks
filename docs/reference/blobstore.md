---
id: blobstore
title: "BlobStore"
sidebar_label: "BlobStore"
---

`zio-blocks` is currently **Project Stage: Development** and should be treated as **not production-ready** unless you have a very clear adoption plan (pinning versions, testing, and tolerating breaking changes).

## Status of BlobStore (Production Readiness)

The `blobstore` module is **new and experimental**.

- **What exists today**
  - A small core API: `BlobStore` (put/get/head/delete/list).
  - Strongly-typed identifiers:
    - `BlobKey` and `BlobKeyPrefix` validated via `zio.prelude.ZValidation`, allowing validation errors to be accumulated.
  - A **test-only** in-memory backend (`InMemoryBlobStore`) living under `src/test` so it is **not published** and cannot be used from production code.

- **What does *not* exist yet (important gaps)**
  - No production-grade backends (S3/GCS/Azure Blob, filesystem, DB, etc.).
  - No streaming APIs (e.g. chunked reads/writes) — current API is byte-array based.
  - No cross-cutting concerns like retries, metrics, tracing, encryption, compression, TTLs, versioning, or multipart uploads.
  - No stability guarantees: the API may change while the module is being designed.

## Design Notes

- **Why `ZValidation`**: key/prefix parsing is modeled as validation so callers can accumulate multiple problems (e.g. empty + illegal prefix character) instead of failing fast.
- **No “unsafe” constructors**: keys/prefixes should come from validation rather than “trust me” constructors. Internally, backends should store already-validated keys, so they never need to re-parse.

## Installation

Add to your `build.sbt`:

```scala
libraryDependencies += "dev.zio" %% "zio-blocks-blobstore" % "@VERSION@"
```

## Roadmap (High-Level)

Likely next steps before this can be considered for production usage:

- Add at least one production backend and a test suite that exercises it.
- Add streaming read/write APIs (avoid loading entire blobs into memory).
- Decide on a stability policy for the module API (and document it).

