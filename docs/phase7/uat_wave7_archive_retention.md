# UAT Wave 7 Archive Retrieval and Retention

## Scope

Wave 7 extends the sealed release-archive workflow with retrieval-ready search,
supersession tracking, and indexed evidence references so operations and audit
teams can locate the right release bundle without unpacking raw manifests.

## Delivered Capabilities

- global admin release-archive retrieval across UAT cycles,
- search filters for cycle, recommended outcome, retention status, superseded
  visibility, and archive/snapshot name matches,
- retention metadata on each release archive with a review timestamp and
  derived lifecycle state,
- supersession tracking so earlier release archives can point to the archive
  that replaced them,
- evidence-item indexing for snapshot payloads, stakeholder acceptance, packet
  distribution, recipient acknowledgement, launch governance, and support
  handoff context, and
- admin API routes to search archives, list evidence items, and record archive
  supersession updates.

## Validation Targets

- archive creation should persist retention review dates and evidence-item
  counts,
- archive search should return active and superseded bundles according to the
  requested filters,
- supersession should be limited to archives from the same UAT cycle and update
  retention status for the superseded archive, and
- evidence-item retrieval should expose audit-ready references without
  requiring manifest inspection.
