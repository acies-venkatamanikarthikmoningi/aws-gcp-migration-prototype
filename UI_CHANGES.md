# Console UI redesign

Only `ui/index.html` changed. The backend, the API and the state format are untouched, and all 31 tests still pass.
The static export (`python -m accelerator.cli export`) and the live API (`uvicorn accelerator.api:app`) both work as before.

## Feature check

Every control, table column, filter and detail field in the old console was listed and checked in the new one with an automated browser test.
Nothing was removed. Three things moved on purpose:

- The 11-step journey strip now shows only on Load runs and Validation.
- The 8 dashboard numbers are in "Progress by stage" on the Overview.
- Bulk and catch-up method on Movement plan, and warning counts on Code conversion, moved into the detail panel and the Findings tab.

## Motion

Short transitions only, all under 0.3 s, and only in response to something the user does: hover, opening a panel, switching page, a confirmation message, zoom buttons on the graph.
Nothing moves on its own. All motion is turned off when the operating system asks for reduced motion.
Measured in Chrome: every screen renders in under 20 ms and the dependency graph holds 60 frames per second while zooming and dragging.

## Round 1: honesty labels

Page only. No backend, API or test changes.

- A "Simulated execution" marker sits in the top bar whenever the project uses synthetic accounts.
- Validation runs that exist to be found are labelled: "Seeded test defect" (faults planted in the Google Cloud copy), "Simulated source change" (lag check) and "Sample mismatch data" (customer files). A banner on the run explains it, and the failing checks that come from the planted faults carry a "Seeded" tag. Overview says the same in the attention list.
- Every validation check shows its evidence level: Aggregate, Content hash, Key-level, Row-level or Metadata. The panel explains what each level does and does not prove.
- Code conversion says "Not run on Google Cloud" above every output, the list says "Static checks pass", and the bar is explained as the share of constructs handled, not correctness.
- Every screen that makes proposals shows "Rules only" beside its title. It changes to "AI-assisted" once any engine run records a language model.
- Overview has "Where things stand": eight areas by five states (validated automatically, needs a manual check, has a discrepancy, awaiting approval, approved to continue). Each count links to its screen, and every status chip carries its state as a tooltip.

## Round 2: projects

What changed

- Projects are real. **New project** (top-bar project menu, or `#new`) creates a project with its own workspace folder, database, upload vault and audit log under `runs/projects/<id>/`. The built-in sample stays as a read-only project beside them.
- The project menu lists every project and switches between them. The choice is remembered in the browser and can be forced with `?project=<id>`.
- Uploads: on a user project the Uploads screen has **Add files to this project** (many files or a .zip, same security checks as before) and **Run assessment**. The older "analyse a file" drop zone stays as a preview that adds nothing.
- Run assessment runs discovery, mapping, conversion, planning, dependencies, risk and e-mail drafts on the uploaded files only. It never approves anything. Re-running rebuilds the results and keeps an earlier mapping decision only when the resource and target are unchanged.
- State is saved to `ctx.json` in the project folder after each change, so a server restart loses nothing.
- Empty states: a project with no files shows a three-step start list; screens that need an assessment say so; Load runs and Validation say plainly that nothing was loaded or compared. The readiness report states the same and has no reconciliation section for such a project.
- The sample-only controls (Reset demo run) show only on the sample project.

New endpoints: `POST /api/v1/projects`, `PATCH /api/v1/projects/{id}`, `POST /api/v1/projects/{id}/uploads`, `POST /api/v1/projects/{id}/assess`, and `?project=<id>` on `/state`, `/audit/verify` and `/reports/{name}`. All earlier endpoints behave as before.

Limits to know about

- The assessment reads files only. There is no AWS or Google Cloud connection, and no source or target data, so a new project has no data-load or reconciliation results until the later rounds add them.
- Project fields (account ID, GCP project) are labels. They are checked for format, not against a real account.
- A project cannot be deleted from the console yet; remove its folder under `runs/projects/`.

## Round 3: data availability check

What it answers: before anything is converted, is the data this application needs already in Google Cloud, and does it cover the same dates?

- New screen **Data availability** (Assess group). Each dataset compares a source CSV (or its staging copy) with a target CSV: total rows, rows by month, days present in the source but missing in the target, differing day counts, missing columns, and, when a key column is named, missing keys. Evidence strength is printed on each result: totals, counts by day, or key-level. Matching counts are labelled as evidence, not proof.
- Outcomes: **Available**, **Differences found**, **Not in target** (no target file yet), **Could not check**. A dataset without a date column gets a total-count check only, and says so.
- **Limited access.** When the original source cannot be seen (customer-owned, offline store files), a dataset is marked "Staging only". Matching counts are then not enough: the dataset stays at **Needs confirmation** until a person records the source owner's reply. **Draft owner e-mail** creates a draft (never sent; visible under Stakeholders) and the reply is recorded with who said what. If the owner reports missing files, the dataset stays unconfirmed.
- **Proceed anyway.** For a difference or a missing target, a person records a decision with their name and a reason of at least 10 characters (Proceed anyway or Hold). It is written to the audit log and the approvals table. If the data changes afterwards, the decision lapses and a new one is needed.
- **Readiness gate DATA_AVAILABILITY** per application: blocked while any dataset needs a check, confirmation or decision; passes when every dataset is clear or accepted by a person (the count accepted despite a difference is shown); shown as open when no datasets exist.
- The readiness report has a Data availability section and an `availability.csv` extract; the workbook has the same sheet.
- Sample project: three seeded datasets show the three outcomes. The target copy is built by the sample on purpose with a missing month and some missing rows, and is labelled as sample data.
- New projects: add the source and target CSVs on Uploads, then **Add dataset**. A target identical to the source is accepted (it is stored once, but can be picked on both sides).

New endpoints: `GET /api/v1/projects/{id}/availability`, `POST /api/v1/projects/{id}/availability/datasets`, `POST /api/v1/projects/{id}/availability/check`, and per dataset `POST /api/v1/availability/{id}/check | target | request-confirmation | confirm | decision`.

Limits: CSV files only (up to 2 million rows per side), ISO dates (2025-01-31) only, and files are compared on the machine running the app. Nothing reads S3, SFTP, BigQuery or any other cloud store; that needs the connector work planned separately.

## Round 4: sign-off, risk follow-up, owner enrichment

What it answers: who is accountable for closing a risk, who has formally approved going live, and what happens when there's no one listed as the owner.

- **Bug fix: duplicate risk rows.** Every re-assessment (approving a mapping, checking availability again, anything that re-ran the engine) was inserting a fresh, randomly-numbered risk row per dimension without removing the old one, so the `risk_items` table grew without bound (seen growing from 49 to 147 rows after two extra runs). Risk rows now use a deterministic id per project/application/dimension, so a re-assessment updates the existing row instead of adding another. This also makes it safe to carry follow-up notes and a manually entered owner across re-assessments without them being overwritten or duplicated.
- **Formal sign-off.** A new readiness gate, **Formal sign-off**, requires a named business approver and a named technical approver, each with a reason of at least 10 characters, before it passes. It only opens for recording once every other gate for that application is clear; while anything else is blocked, it stays open with "Other gates must clear first." Recorded on the Readiness screen, in the app detail panel. If anything else about the application's gates changes after both have signed off, the gate goes back to blocked ("Other gates changed since this was signed off. Sign off again.") rather than silently staying passed — it is a two-role approval, not a one-time checkbox.
- **Risk follow-up.** Each risk dimension (service compatibility, data quality, security, etc.) on the Readiness screen now has an editable tracking status (Open, In progress, Mitigated, Accepted (low)), an owner, and a free-text note, independent of the automatically computed score. This is how a person records "someone is on this" without the score itself changing.
- **Owner enrichment.** On the Stakeholders → Owner directory tab, any application without a verified owner gets an "Enter owner" button; applications with a directory-sourced owner get "Change owner". A manually entered owner is recorded separately from the ownership directory, takes priority over it, is marked "Entered manually by …", and can be cleared to fall back to the directory. This is what unblocks the OWNER_APPROVAL / DOWNSTREAM_OWNERS_KNOWN gates for a project that has no ownership CSV at all.
- Sample project: the two intentionally seeded demo blockers (a failed reconciliation check, a hardcoded-credential finding) still correctly block Formal sign-off even after mappings are approved and owners are set — sign-off isn't meant to be reachable for a deliberately broken demo app, and this is confirmation the gate logic holds, not a bug.
- A new, non-sample project with a database/storage/analytics resource previously stayed blocked on "Mandatory reconciliation: validation NOT RUN for data objects" with no way to clear it — **Round 5 below is what fixes that.**

New endpoints: `POST /api/v1/projects/{pid}/applications/{app}/signoff`, `POST /api/v1/projects/{pid}/applications/{app}/risk/{dimension}/followup`, `POST` and `DELETE /api/v1/projects/{pid}/applications/{app}/owner`.

## Round 5: your own data validation, exceptions queue, monitoring notes

What it answers: Round 4 confirmed that a custom project has no way to clear the Mandatory reconciliation gate — this is that way. The sample project's full post-migration reconciliation (table/partition/hash-level checks across the scripted demo storyline) is untouched and still lives on the Validation runs tab; everything below is new and only applies to a project you create yourself.

- **Your objects (Validation → "Your objects" tab).** Add an object by naming a source CSV, a target CSV, and a shared key column (both files must already be on Uploads). Running it compares row count, key coverage, and per-column totals, with optional per-column PII masking (values are compared as hashes, never shown in full) and a settable numeric tolerance per column (so "within $0.01" or "within 5 units" counts as a match instead of requiring an exact one). This reuses the exact same reconciliation engine the sample's post-migration run uses — it is the same evidence the readiness gate asks for, just sized for one or two files you supply rather than a full scripted migration.
- **Clearing the gate.** Linking an object to an application and getting it to PASS is what makes the Mandatory reconciliation gate for that application clear (the gate simply disappears from the list, the same way every other cleared gate in this tool does — it was never shown as a green "Passed" row, see Round 1/2 notes). A FAIL keeps the gate blocked and names the object and the failing checks in the gate's own detail text.
- **Fixing a FAIL.** Change which uploaded file is the target (or adjust the tolerance) and press Re-run; the object's status updates immediately and the gate re-evaluates on the next state load.
- **Exceptions queue (Validation → "Exceptions queue" tab).** Every check failure across every validation run (the sample's and your own objects' alike) lands here as one row, with a manually tracked status (Open, In progress, Resolved), an owner, and a note — independent of the check's own pass/fail outcome. A check that was waived through the sample's own waiver workflow, or one that passed on an automatic retry, shows here as closed by the system and can't be edited by hand (editing one returns a plain error naming which system-closed state it's in) — manual tracking is for the ones still open, not for relitigating what the engine already resolved.
- **Post-cutover monitoring note.** A free-text note per application, for what to watch after go-live and for how long. Lives both on the Readiness screen's per-application detail panel and on any "Your objects" entry linked to that application (same note, shown in both places); saved with a timestamp and who saved it.
- A sample-project attempt to add a validation object is refused with a plain explanation (the sample is read-only), the same as every other "add" action on that project.

New backend: `accelerator/dq.py` (validation objects + exceptions, reusing `validation.py`'s existing reconciliation engine unchanged). New endpoints: `GET`/`POST /api/v1/projects/{pid}/validation/objects`, `POST /api/v1/validation/objects/{oid}/target`, `POST /api/v1/validation/objects/{oid}/tolerances`, `POST /api/v1/validation/objects/{oid}/rerun`, `GET /api/v1/projects/{pid}/validation/exceptions`, `POST /api/v1/projects/{pid}/validation/exceptions/{eid}`, `GET`/`POST /api/v1/projects/{pid}/applications/{app}/monitoring-note`.

**Known gap (fixed after Round 5, see below):** the Data availability "Add dataset" form had no application field.

## Fix: data availability datasets linked to the right application

- **What was wrong.** "Add dataset" had no application field, and the API silently filled in the project's own name. Every readiness gate is keyed by the application tag your resources carry (from Terraform tags or inference), so for any project whose name differs from its applications, a dataset counted toward no gate at all and the Data availability gate stayed "no datasets added" forever.
- **Now.** The form has an Application list built from what the assessment found. It pre-selects the project's configured application if the assessment found it, otherwise the only application found; with several applications and no match it is left "Not linked yet" rather than guessed. The API does the same when no application is sent, and refuses a name the assessment did not find (with the list of valid names in the error).
- Datasets that are not linked to a known application show "Not linked" in amber in the list and a warning in the side panel, which also has a Link control to fix it. Existing datasets on disk that carry the old project name show up this way, so they can be re-linked.
- With more than one application, an unlinked dataset counts toward none, and the gate says so ("1 dataset(s) are not linked to an application yet") instead of "no datasets added". With a single application there is nothing to be ambiguous about, so it counts for that one.
- The validation-object form (Round 5) now pre-selects the same default and the API validates the name the same way.
- **Persistence fix found on the way:** after a data availability or validation-object action the gates were recomputed but not saved, so a server restart showed the earlier gate state until the next action. They are now saved.

New endpoint: `POST /api/v1/availability/{dataset}/application`.

## Round 6: communications and dependencies

What it answers: who was told what and what did they say, where each application runs now, and whether every application that reads this one's data is ready for it to move.

**Two bugs fixed first, because Round 6 depends on them**

- **E-mail drafts accumulated.** Every re-evaluation (approving a mapping, adding a dataset, anything) inserted a fresh copy of every draft with a new random id: 40 drafts became 120 after two re-evaluations. Drafts now have a stable id per recipient, event and subject application and are updated in place. What a person did to a draft (edit, approval, sent, reply) is kept; drafts that are no longer generated are removed unless someone already acted on them, in which case they stay as history. A full re-assessment keeps them too.
- **Data CSVs were read as AWS inventory.** Any CSV uploaded for the availability check or a validation object (for example `id,dt,amt`) was classified by extension alone as a resource inventory, so every row became a "resource" with a service mapping (a 60-row file added 60 resources and 60 pending mappings). A CSV is now an inventory only if its header has a service or resource-type column; otherwise it is a data file and is not parsed for resources. Projects uploaded before this fix are re-checked the next time the assessment runs.

**E-mail lifecycle (Stakeholders, Email drafts)**

- Each draft now has an approval state and a delivery state. Filters: Needs approval, Approved not sent, Sent no reply, Replied or bounced, Owner unknown.
- **Approve or reject** a draft with your name. **Edit** the subject and message; the tool's own draft is kept underneath and keeps updating. An approval covers the exact text approved: editing it, or the tool regenerating it with new numbers, shows "Changed since approval" and it must be approved again.
- **Record as sent** (who, and how) is only possible for currently approved text. The text as sent is frozen and shown as "As sent". The tool still sends nothing; this records that a person sent it from their own mail client.
- **Record what came back**: a reply (Acknowledged, Question or Objection, with what they said, several replies kept in order) or a bounce. The panel shows the full history. An objection also appears at the top of Overview's attention list.
- The five-state summary on Overview now counts e-mails by approval state.

**Where each application runs (Dependencies, new Applications tab)**

- Every application in the dependency graph has a recorded status: Still on AWS, Cutover in progress, or On Google Cloud, with who recorded it and a note of at least 10 characters. Rolling back to "Still on AWS" is allowed with a note.
- For an application in this assessment, cutover can be recorded as started only once it is signed off (Round 4's two-role sign-off), and as complete only after it was recorded as started. An application outside the assessment (a downstream consumer) can be recorded as whatever its team says.
- **"Migration started" and "Migration completed"** e-mails (the two events the blueprint listed and the tool did not produce) are drafted when a start or completion is recorded: one to the application's own owner, and one to the owner of every application that reads its data, which says whether a bridge was agreed for that application. They go through the same approve, send and reply steps.
- Graph nodes whose application is on Google Cloud or mid-cutover carry a small green or amber marker.

**Owner-confirmed links (Dependencies, Graph)**

- The stored graph mixed directions: `READS_FROM` points from the reader to what it reads, `DATA_SOURCE` from the source to the reader, and Terraform references from the referrer to what it references, which may be either way round in data terms. Arrows, "Depends on / Used by", impact lists and risk scores now all follow the direction data flows.
- Every link in a resource's side panel says which way data flows, and inferred links say "direction not confirmed". **Record answer** takes the owner's name, what they said, and one of: data flows this way, data flows the other way, or not a real link. A link an owner says is not real is drawn faint red, and stops counting for impact and gates. Answers can be changed or cleared.

**New gate: Downstream ready (Readiness)**

- An application should not be cut over while an application that reads its data is still on AWS. The gate is blocked, naming those applications, unless each is on Google Cloud or its owner agreed a **bridge** (dual run, copy back), recorded on the Applications tab with who agreed and how it works.
- If the only links to other applications are inferred and unconfirmed, the gate is open ("need an owner to confirm which way data flows"), not guessed either way.
- Sample project: CustomerAnalytics, LegacyBilling and ConnectedDevices now also show this gate as blocked, because SalesReporting, CustomerPortal and FinanceOps read their data and nothing says they are ready. That is the gate working, not a regression. Because this is a new gate, a sign-off recorded before this round for an application with downstream consumers no longer matches its gates and needs to be recorded again.

New backend: `accelerator/comms.py`. New endpoints: `POST /api/v1/projects/{pid}/notifications/{id}/approve`, `.../edit`, `.../track`; `POST /api/v1/projects/{pid}/applications/{app}/migration-status`; `POST /api/v1/projects/{pid}/dependencies/confirm`; `POST` and `DELETE /api/v1/projects/{pid}/applications/{app}/bridges/{downstream}`.

## Round 7: load controls and conversion extras

What it answers: how much data moved and what that costs, whether the load stayed within its limits and whether the schema moved under it; and, for converted code, which version is current, who reviewed it, and what the converter's own checks missed.

**Load runs and Movement plan**

- **Volume and cost.** Each run shows the data it moved (from the batch manifests the engine already writes), elapsed time, and an estimated AWS transfer-out cost. The Movement plan has a new "Planned volume and cost" box: total size from the inventory, online transfer time at the project's bandwidth, and estimated AWS transfer-out, Cloud Storage landing and BigQuery storage cost. Costs are estimates at list price from `knowledge_base/cost_rates.json`, which names its sources (AWS EC2/data-transfer and Google Cloud Storage and BigQuery pricing pages) and the date they were checked (25 Nov 2026). The AWS free 100 GB a month is not subtracted, so it is an upper estimate; BigQuery batch loading is free and its first 10 GB of storage is subtracted. Edit the file if your region or contract differs. The sample's 7,841 GB plan comes to about $706 AWS transfer-out; its actual 362 KB run to under one cent.
- **Concurrency limit.** "Batches at a time" is now a setting on the load plan (1 to 8, with who changed it and why), and the engine uses it for the next run instead of a fixed 4. Each run records the limit in force and the most batches that were actually in flight at once, measured from new per-batch start and end times (the sample backfill: 4 of 4). Runs recorded before this version have no timings and say so. Starting or retrying a run from the screen is still not built: runs happen only inside the sample's scripted story, and the screen says this plainly.
- **Schema drift.** Load runs now checks the columns the load copies against the simulated source and target tables, and against the source columns captured when the plan was made (new). The three lineage columns the loader adds to the target are listed as expected. No drift in the sample; a column added to the source afterwards is reported (covered by a test).
- **Data availability, drift between checks.** Each dataset remembers the columns seen at each check. When a re-check sees columns added or removed compared with the previous check, the dataset shows "Columns changed" and the panel says what changed and since when. The re-check form can now replace the source file as well as the target, in one step.
- The availability table lost a column to make room: source access is now a line under the dataset name, and source/target row counts share one column. The Application column only appears when the datasets span more than one application or one is not linked.

**Code conversion**

- **Generated tests (new Tests tab).** Static tests that go beyond the converter's own syntax checks, aimed at what the earlier stress test found: names used but never defined (an import the conversion removed), AWS SDK imports left behind, top-level functions that disappeared, Terraform variables used but not declared, AWS resources left in Terraform, tables lost from SQL. Each says where it ran: "Here, reading the files" (nothing is executed) or "Not run: needs your environment" (Terraform validate/plan, warehouse parity). Each conversion also gets a test file for the engineer's repository (`tests/test_<file>_generated.py`, `tests/test_<file>_terraform.py`) or a parity SQL file (`tests/parity_<file>.sql`, clearly marked NOT RUN); they are in the per-file download and the zip. Failed generated tests count against the testing-coverage risk score.
- On the sample these tests find a real problem the converter's checks passed: the CloudFormation output uses `var.project_id` and `var.source_bucket` without declaring them, so `terraform validate` would fail. On the files from the earlier stress test they catch the Glue job's missing `glueContext` and transforms, the Lambda's missing `gcp_compat` import and leftover `boto3`, and the undeclared `migrated_image_web`.
- **Version history (Versions and review tab).** Every assessment run stores each conversion's files as a version with a content hash. A person can **Edit** an output file (Code tab); saving creates the next version with the other files carried over, and the tests run again on it. Any earlier version can be compared with the current one as a line diff. Versions generated before this round show "content not kept" and cannot be compared or reviewed.
- **Review (approve or reject a version).** Round 4 listed "approve or reject generated code" but it was never built; it is here now. A review needs a name and a note of at least 10 characters, applies only to the latest version, and covers exactly that version's files. A later version with different content needs its own review; a regenerated version identical to an approved one keeps the approval ("Carried over from version N"). The artefact list and header show the version number, whether it was edited, and the review state.

New backend: `accelerator/codecheck.py`, `accelerator/codehistory.py`, `accelerator/loadops.py`, `knowledge_base/cost_rates.json`. New endpoints: `POST /api/v1/projects/{pid}/conversions/{id}/review`, `POST /api/v1/projects/{pid}/conversions/{id}/edit`, `GET /api/v1/projects/{pid}/conversions/{a}/diff/{b}`, `POST /api/v1/projects/{pid}/load-controls`, `POST /api/v1/availability/{dataset}/source`.

**Still not built from the original plan:** approving a load plan and starting or retrying load runs from the screen (Round 4). Without a real source and target connection it would only replay the sample's simulated data, so it waits for the connector track.

## Round 8: incremental loads shown on Load runs

What it answers: the console showed the historical backfill prominently, but the incremental loads that kept the Google Cloud copy current were only plain rows in the runs table. They happened and were recorded; the screen did not say so.

- **New section "Historical and incremental loads"** above the load controls: three cards (Historical backfill, Incremental catch-up, Change events (CDC)). Each shows status, rows loaded or events applied, the position reached (watermark or log position), and the audit entry that recorded it (event name, entry number, time, hash prefix). Selecting a card opens that run below.
- **Evidence that the gap closed.** A line under the cards quotes the two existing freshness checks: before the incremental loads the Google Cloud copy was behind AWS (the sample: 5,354 minutes), and after catch-up and CDC the final check passes with a lag of 0. Both are on the Validation screen.
- **"Recorded" box on incremental runs.** Catch-up: watermark moved from and to, rows pulled, inserted, updated, rejected, merge method, audit entry. CDC: events received, applied, inserts, updates and deletes, events skipped as already applied, log position from and to, source commit times, audit entry. The replay run shows a "Replay check" note: the same events were delivered again and none were applied twice.
- **Labels.** The incremental and CDC cards and boxes carry "Simulated source change", as elsewhere: the source changes come from sample files, not a live AWS feed.
- **Readable names.** Run types read "Historical backfill", "Incremental catch-up", "Change events (CDC)" instead of "Cdc apply". For CDC runs, rows extracted and loaded now show the event counts instead of a dash.
- **One backend change, small.** CDC runs now record the log position range, the position before the run, the events received and the commit time range in the batch checkpoint, and a replay with nothing new is labelled "no new events (already applied up to lsn 9005)" instead of the backwards range `lsn 9006..9005`. Runs saved before this show what they have and a dash for the rest. Nothing else in the engine changed. New test: `test_incremental_and_cdc_loads_are_recorded_for_the_console`.

Not changed: starting or retrying runs from the screen is still not built, and there is no button to simulate a source change.

## Round 9: Load runs tidy-up, Reports order, search almost everywhere

- **Watermark removed from Load runs.** It no longer appears in the Historical and incremental loads cards, as a column in the runs table, in the incremental run's "Recorded" box, or in the journey strip (which now says "Incremental and CDC, replay-safe"). The incremental run's batch is labelled "Changed after the snapshot" instead of showing its timestamp. This is display only: the stored record, and the raw checkpoint JSON in the batch panel, still hold the watermark fields.
- **"Run" is now "Run ID"** in the runs table.
- **Batches are a table, not a grid.** Batch, Partition, Status, Rows loaded, Quarantined and Attempts, sortable, with a search box, in a scrollable area with a fixed header (about ten rows tall, so 24 batches no longer push the page down). Selecting a row opens the batch panel as before. A new run starts with an empty search.
- **Reports and audit order.** The runs section comes first (tabs: Engine runs, Approvals, Audit log, with Engine runs open by default), then Audit integrity, then Exports at the bottom, now with its own search.
- **Search.** Every table drawn by the shared table component now has a "Search this table" box. Every word typed must appear somewhere in what the row shows (the text of its cells), and an "N of M" count appears while searching. Tables that already had their own search keep it: Uploads, Discovery, Service mapping, and the Audit log. Also added: a "Find a resource" box on the Dependencies graph (matching boxes stay bright, the rest fade; Enter opens the first match), Search exports, and Search artefacts on Code conversion.
- **Not covered.** The small tables printed inside side panels (findings, requirements, needs review, generated tests, versions, checks, mapping log, data-quality results, gates) are static and cannot be searched yet.
- **No backend or API change.**

## Design basis

- Layout follows the patterns cloud consoles use for this kind of work (AWS Cloudscape, Google Cloud Migration Center, IBM Carbon tables):
  grouped side navigation, a page header with one line of description, tables with sort and pagination, and a side panel for the selected row.
- Visual style: IBM Plex Sans and Plex Mono, neutral greys, one blue for actions, colour used only for status.
  Corners are 2 to 3 px, there are no shadows on content, no gradients and no purple.
- Status is shown as a small square plus a word (Passed, Failed, Pending), never by colour alone.

## Whole app

- Navigation is grouped by stage: Overview, Assess, Build, Move data, Go live. Each item shows a live count, red or amber when something needs action.
- The top bar has a project menu with all project details (accounts, regions, bandwidth, catalogue version).
- "Re-run demo" is now "Reset demo run", shown only in live mode and confirmed before it runs.
- The 11-step journey strip appears only on Load runs and Validation, where it belongs, instead of on every screen.
- Browser `prompt()` pop-ups are replaced by labelled forms in the side panel, with validation messages.
- Actions no longer reload the page: data refreshes in place and a confirmation message appears.
- Every table sorts by column and pages at 25 rows. Every list has search and filters with counts.
- Screens render on demand instead of all at once.

## Screen by screen

| Screen | Before | Now |
| --- | --- | --- |
| Overview | 8 mixed KPI tiles, long run transcript | Cutover verdict, "Needs attention" list with links, progress by stage, applications table, strategy table, run log collapsed |
| Uploads | Category counts as tiles, analysis result buried | Accepted / rejected filter, category menu, search, file details panel, analysis preview panel |
| Discovery | Pills, cramped drawer | Filter with counts, side panel with source-vs-source conflict comparison, impact, evidence table, configuration |
| Dependencies | Labels cut at 20 characters, overlapping edges, no zoom | Graph and Applications tabs; arrows follow data flow; pan and zoom; owner answers on each link; where each application runs; bridges for downstream applications |
| Service mapping | `prompt()` approvals, catalogue at bottom | Pending first, approve or reject form with rationale, last decision shown, catalogue in its own tab with search highlighting |
| Code conversion | Table plus dropdown, no download | Artefact list with version and review state, file tabs, line numbers, changed source lines highlighted, generated tests, versions with diff, edit and review, download per file or as a zip with test files |
| Movement plan | 8-column table | Clear downtime gaps, side panel with reasons and rollback, waves in dependency order, backfill plan |
| Load runs | Batch dialog | Load controls (batches at a time), schema check, run metrics with data moved, peak parallelism and estimated cost, batch grid with retries and quarantine marked, batch panel with counts, checksum, landing path and checkpoint |
| Validation | Long table | Tabs: Validation runs (AWS vs Google Cloud vs difference summary, filters, side-by-side values, waiver form), Your objects (add and re-run your own source/target CSV reconciliation, tolerances, PII masking), Exceptions queue (cross-run remediation tracking) |
| Readiness | 7 tall cards | Ranked applications table and one detail view: gates first, then score breakdown |
| Stakeholders | Clipped owner table | Drafts and owner directory as tabs; approve, edit, record sent, reply or bounce for each e-mail, with history |
| Reports and audit | Full 124-row log dumped | Exports with descriptions, chain status, approvals / audit log / engine runs as tabs, searchable log, event detail |
