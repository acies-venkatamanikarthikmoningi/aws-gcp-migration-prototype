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
| Dependencies | Labels cut at 20 characters, overlapping edges, no zoom | Larger nodes, column headings, pan and zoom, selected node highlights its links and dims the rest, owners in panel |
| Service mapping | `prompt()` approvals, catalogue at bottom | Pending first, approve or reject form with rationale, last decision shown, catalogue in its own tab with search highlighting |
| Code conversion | Table plus dropdown, no download | Artefact list, file tabs, line numbers, changed source lines highlighted, findings and checks tabs, download per file or as a zip |
| Movement plan | 8-column table | Clear downtime gaps, side panel with reasons and rollback, waves in dependency order, backfill plan |
| Load runs | Batch dialog | Run metrics, batch grid with retries and quarantine marked, batch panel with counts, checksum, landing path and checkpoint |
| Validation | Long table | AWS vs Google Cloud vs difference summary, filters by result and check type, side-by-side values, waiver form with date picker |
| Readiness | 7 tall cards | Ranked applications table and one detail view: gates first, then score breakdown |
| Stakeholders | Clipped owner table | Drafts and owner directory as tabs, email shown as a readable message, copy button |
| Reports and audit | Full 124-row log dumped | Exports with descriptions, chain status, approvals / audit log / engine runs as tabs, searchable log, event detail |
