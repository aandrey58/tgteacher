# TGTeacher MCP v1.0 — approved roadmap

Target: **45 distinct MCP tools**. Stage 4 establishes 33 implemented tools; the remaining 12 are planned, not exposed as placeholders.

| # | Tool | Stage | Status | Purpose |
|---:|---|---|---|---|
| 1 | `list_families` | Stage 1–3 — Catalog | implemented | List catalog groups |
| 2 | `get_family` | Stage 1–3 — Catalog | implemented | Read current group and task IDs |
| 3 | `search_words` | Stage 1–3 — Catalog | implemented | Search words and translations |
| 4 | `get_content_format` | Stage 1–3 — Catalog | implemented | Read authoring template |
| 5 | `create_draft` | Stage 3 — Creation drafts | implemented | Create new-group draft |
| 6 | `list_drafts` | Stage 3 — Creation drafts | implemented | List creation drafts |
| 7 | `get_draft` | Stage 3 — Creation drafts | implemented | Read creation draft |
| 8 | `update_draft` | Stage 3 — Creation drafts | implemented | Replace draft TXT |
| 9 | `validate_draft` | Stage 3 — Creation drafts | implemented | Check publication readiness |
| 10 | `publish_draft` | Stage 3 — Creation drafts | implemented | Publish new group |
| 11 | `archive_family` | Stage 3.1 — Archive | implemented | Hide group without losing progress |
| 12 | `restore_family` | Stage 3.1 — Archive | implemented | Restore group |
| 13 | `create_edit_draft` | Stage 3.2 — Edit drafts | implemented | Clone current published version |
| 14 | `list_edit_drafts` | Stage 3.2 — Edit drafts | implemented | Find edit drafts |
| 15 | `get_edit_draft` | Stage 3.2 — Edit drafts | implemented | Read edit draft |
| 16 | `update_edit_draft` | Stage 3.2 — Edit drafts | implemented | Replace draft JSON |
| 17 | `preview_edit` | Stage 3.2 — Edit drafts | implemented | Check change impact |
| 18 | `publish_edit` | Stage 3.2 — Edit drafts | implemented | Publish a reviewed revision |
| 19 | `list_family_versions` | Stage 3.2 — Versions | implemented | List immutable versions |
| 20 | `get_family_version` | Stage 3.2 — Versions | implemented | Read version snapshot |
| 21 | `rollback_family` | Stage 3.2 — Versions | implemented | Restore historical version as a new revision |
| 22 | `delete_draft` | Stage 3.3 — Content Operations | implemented | Discard unpublished creation draft |
| 23 | `delete_edit_draft` | Stage 3.3 — Content Operations | implemented | Discard unpublished edit draft |
| 24 | `add_tasks` | Stage 3.3 — Content Operations | implemented | Insert up to 100 draft tasks |
| 25 | `update_task` | Stage 3.3 — Content Operations | implemented | Patch one draft task |
| 26 | `archive_task` | Stage 3.3 — Content Operations | implemented | Remove task from draft; archive on publication |
| 27 | `reorder_tasks` | Stage 3.3 — Content Operations | implemented | Reorder draft tasks without changing IDs |
| 28 | `list_tts_providers` | Stage 4 — TTS | implemented | Discover configured providers |
| 29 | `list_voices` | Stage 4 — TTS | implemented | Discover voice options |
| 30 | `estimate_tts_cost` | Stage 4 — TTS | implemented | Estimate provider cost before generation |
| 31 | `generate_audio` | Stage 4 — TTS | implemented | Generate one audio asset |
| 32 | `batch_generate_audio` | Stage 4 — TTS | implemented | Generate bounded batch |
| 33 | `attach_audio` | Stage 4 — TTS | implemented | Attach existing audio to draft task |
| 34 | `generate_image` | Stage 5 — Images | planned | Generate one image asset |
| 35 | `batch_generate_images` | Stage 5 — Images | planned | Generate bounded batch |
| 36 | `attach_image` | Stage 5 — Images | planned | Attach image to draft task |
| 37 | `list_media` | Stage 5 — Images | planned | List accessible assets |
| 38 | `get_media` | Stage 5 — Images | planned | Read asset metadata |
| 39 | `detach_media` | Stage 5 — Images | planned | Remove draft attachment without deleting history |
| 40 | `get_learning_progress` | Stage 6 — SRS | planned | Read authorized learner progress |
| 41 | `get_learning_statistics` | Stage 6 — SRS | planned | Read authorized learning statistics |
| 42 | `get_weak_words` | Stage 6 — SRS | planned | Identify weak vocabulary |
| 43 | `get_due_reviews` | Stage 6 — SRS | planned | Get scheduled reviews |
| 44 | `get_learner_profile` | Stage 7 — Adaptive Learning | planned | Read authorized learner profile |
| 45 | `update_learning_preferences` | Stage 7 — Adaptive Learning | planned | Update authorized learning preferences |

## Milestones

| Milestone | Total |
|---|---:|
| Stage 3.2 | 21 |
| Stage 3.3 | 27 |
| Stage 4 | 33 |
| Stage 5 | 39 |
| Stage 6 | 43 |
| Stage 7 | 45 |

## Contract and security

- Current scopes: content:read, content:edit, content:publish. Future learner tools require separate learner authorization and data isolation; content credentials alone must not grant access to personal learner data.
- No planned tool is exposed before its implementation, authorization and tests are ready.
- All draft writes require expected_revision. Publish/rollback are explicitly approved and preserve historical task IDs, versions and answers.
- Stage 3.3 supports draft_kind=new (TXT) and draft_kind=edit (JSON). Stage 1 is vocabulary; stages 2–8 are exercises. Task indices and insertion positions are zero-based, scoped to the specified stage and revision. Reorder accepts a full permutation. New tasks omit task_id; patch cannot change it. Draft deletion uses tombstones, hides drafts from list results and prevents UUID reuse; direct get returns deleted status. Published drafts cannot be deleted. Explicit confirmation is required for deletion and task removal.
- Granular mutations validate the entire resulting draft, use compare-and-swap and return revision plus task references rather than full content. New-group TXT is canonicalized on granular mutation (comments/formatting are not preserved); use update_draft to edit incomplete raw TXT. Stage 7 generated audio is now supported; new images remain blocked until Stage 5.
- Media generation must enforce quotas, authorization, cost limits and safe storage; no live provider requests in CI.
- Every stage must add tests and pass GitHub Actions on PostgreSQL before being marked implemented.

- Stage 4: two configured adapters (OpenAI/ElevenLabs); reader discovery/estimates, editor generation/attachment, publisher publication. MP3 assets are immutable SHA-256-addressed PostgreSQL bytes. Cache keys include provider/model/voice/text/format. Paid generation uses explicit approval, per-call ceiling and atomic UTC-day reservations; failed or pending requests are not automatically retried. Costs are configured estimates, not verified provider invoices. History is retained in family_audio_versions and version snapshots.
