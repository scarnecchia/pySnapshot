# SCDM Snapshot DB — Python Port Design

## Summary

The project ports a monolithic DuckDB SQL script (`scdm_snapshot_db.sql`) into a `pip`-installable Python CLI named `scdm-snapshot-db`. The existing SQL file remains the reference artifact and is not modified; the port's only mandate is behavioural fidelity — the same inputs must produce row-count-identical parquet outputs.

The architecture centres on a table-module registry: each SCDM data domain (enrollment, demographic, dispensing, encounter, lab, vitals, death, mil) is a Python module that implements a uniform `Domain` protocol. A central runner resolves which source tables are present at runtime via an IO probe, topologically sorts the active domains by declared dependencies, and executes them in order against a single persistent DuckDB connection. Config is loaded from TOML, with CLI flags taking precedence over file values and file values taking precedence over built-in defaults, using pydantic-settings for validation. The most significant structural deviation from the original SQL is the replacement of all `CREATE OR REPLACE TEMP TABLE` statements with regular persistent tables in a named `.duckdb` file; this is the primary mechanism by which the port remains viable on memory-constrained hosts, because DuckDB's buffer manager pages regular tables to disk under memory pressure whereas TEMP tables do not. An additional escape hatch — chunked enrollment processing — is available via config for sites where even that is insufficient.

## Definition of Done

- `pip install .` produces a working `scdm-snapshot-db` CLI on the target host (Python >=3.11, no `uv` required).
- `scdm-snapshot-db run --config <file> --dry-run` parses config, probes tables, prints the plan it would execute, and exits 0 without writing any outputs.
- `scdm-snapshot-db validate-config --config <file>` parses and validates the config and exits non-zero on any validation error.
- `scdm-snapshot-db run --config <file>` executes against a declared SCDM site and produces the full set of expected parquet outputs (up to 16 files, depending on which optional tables are available) under `<output_dir>/<dpid>/<request_id>/`, plus `run.log` and `manifest.json`.
- Required tables (`enrollment`, `demographic`) missing at runtime cause an immediate fail-fast with exit code 2 and a clear error log.
- Optional tables (`dispensing`, `encounter`, `lab`, `vitals`, `death`, `mil`) missing at runtime cause the associated domain(s) to be skipped with a warning; the run continues and exits 0 if all remaining domains succeed.
- Schema-drift on any declared table (source parquet exists but lacks a column the port reads) is detected by the runtime probe and handled identically to a missing file (warning + skip for optional, fail-fast for required).
- Domain-level failure isolation: a runtime exception in one optional domain does not halt the run; other domains continue, partial outputs of the failing domain are preserved on disk, and `manifest.json` records `status: "failed"` with the error.
- `--debug` flag (or `[runtime] debug = true`) preserves the working `.duckdb` file and spill directory after the run for post-mortem inspection. Default behaviour deletes both.
- CLI flags override TOML config values, which override built-in defaults.
- `request_id` is validated against a flexible regex supporting 3–6 character DPIDs and `t##`/`b##`/`v##` version suffixes; a mismatch between `request.dpid` and the DPID segment inside `request.request_id` fails at config load.
- DuckDB session is opened against a persistent `.duckdb` file in a configured `work_dir` with `memory_limit`, `threads`, `temp_directory`, and `preserve_insertion_order=false` applied before any query runs.
- Enrollment bridging has an opt-in `chunked` escape hatch (config-driven, default false) for memory-constrained hosts where the default window-function path OOMs.
- Logging produces structured JSON lines to stdout and a human-readable `run.log` in the output directory, both carrying `request_id` on every record.
- Automated test suite uses pyarrow-synthesized fixture parquets and covers: happy path, missing-optional, missing-required, schema drift, failure isolation, debug flag, and dry-run.
- Row counts and spot-checked values from the python port match the original `scdm_snapshot_db.sql` for at least one real site with the same input and config (manual validation, outside CI).
- README rewritten to cover install, config reference, CLI usage, output layout, and the "if enrollment OOMs, set chunked=true" playbook.
- The original `scdm_snapshot_db.sql` remains in the repo as a reference artifact and is not deleted as part of this port.

## Acceptance Criteria

### python-port.AC1: Package installs and CLI is discoverable
- **python-port.AC1.1 Success:** `pip install .` from the repo root succeeds on Python 3.11 with no extras needed
- **python-port.AC1.2 Success:** `pip install .` succeeds on Python 3.12 and 3.13
- **python-port.AC1.3 Success:** `scdm-snapshot-db --help` prints both `run` and `validate-config` subcommands with their flag summaries
- **python-port.AC1.4 Failure:** `pip install .` on Python 3.10 raises a clear error citing the `>=3.11` requirement
- **python-port.AC1.5 Success:** `uv sync` works as a dev-convenience alternative to pip (documented, not CI-enforced)

### python-port.AC2: Config loading and precedence
- **python-port.AC2.1 Success:** A valid TOML config with all required fields loads into `Settings` without error
- **python-port.AC2.2 Success:** A CLI flag override (e.g., `--memory-limit 8GB`) wins over the same field in the TOML file
- **python-port.AC2.3 Success:** A field set in TOML but not on the CLI uses the TOML value
- **python-port.AC2.4 Success:** A field absent from both TOML and CLI uses the built-in default if one exists (`duckdb.threads=2`, `duckdb.preserve_insertion_order=false`, `logging.level="INFO"`, `runtime.debug=false`)
- **python-port.AC2.5 Failure:** A field with no default (e.g., `request.dpid`) absent from both TOML and CLI raises a validation error and exits 2
- **python-port.AC2.6 Failure:** A malformed TOML file raises a parse error and exits 2

### python-port.AC3: Request ID and DPID validation
- **python-port.AC3.1 Success:** `soc_dgr_wp001_mkscnr_v01` is accepted as a valid request_id
- **python-port.AC3.2 Success:** `cder_foo_wp42_abc_t03` (3-char dpid, test version) is accepted
- **python-port.AC3.3 Success:** `soc_bar_wp001_mkscnr_b05` (beta version) is accepted
- **python-port.AC3.4 Failure:** `SOC_DGR_WP001_MKSCNR_V01` (uppercase) is rejected
- **python-port.AC3.5 Failure:** `soc_dgr_wp001_ab_v01` (2-char dpid, too short) is rejected
- **python-port.AC3.6 Failure:** `soc_dgr_wp001_mkscnr_x01` (bad version prefix) is rejected
- **python-port.AC3.7 Failure:** A config with `request.dpid = "MKSCNR"` and `request.request_id = "soc_dgr_wp001_abcdef_v01"` fails at load with a clear dpid-mismatch error (case-insensitive comparison)

### python-port.AC4: Required-table fail-fast behaviour
- **python-port.AC4.1 Failure:** Missing `enrollment` parquet causes exit code 2, logs an ERROR identifying enrollment as the missing required table, and writes no output parquets
- **python-port.AC4.2 Failure:** Missing `demographic` parquet (with enrollment present) causes exit code 2, logs an ERROR identifying demographic, and writes no output parquets
- **python-port.AC4.3 Failure:** Required table present but missing a column from `EXPECTED_COLUMNS` is treated as unavailable, logs an ERROR listing the missing columns, and exits 2

### python-port.AC5: Optional-table skip-and-warn behaviour
- **python-port.AC5.1 Success:** Missing `lab` parquet causes lab domain to be skipped with a WARNING log, other domains run, exit code 0
- **python-port.AC5.2 Success:** Multiple optional tables missing (e.g., lab + vitals + death) all skipped with warnings; remaining domains run
- **python-port.AC5.3 Success:** Optional table present but missing a column is treated as unavailable, logged as schema drift, domain skipped, run continues
- **python-port.AC5.4 Success:** A site with enrollment + demographic + mil only (no other optionals) produces exactly the enrollment, demographic, and mil parquets; exit 0

### python-port.AC6: Happy-path full execution
- **python-port.AC6.1 Success:** A fixture site with all 8 declared tables present produces all 16 expected parquet outputs at `<output_dir>/<dpid>/<request_id>/`
- **python-port.AC6.2 Success:** All 16 outputs have non-zero row counts (assuming fixture data is non-empty)
- **python-port.AC6.3 Success:** `run.log` exists at `<output_dir>/<dpid>/<request_id>/run.log` and contains human-readable entries
- **python-port.AC6.4 Success:** `manifest.json` exists and lists all 8 domains with `status: "ok"`, row counts, and durations
- **python-port.AC6.5 Success:** Exit code is 0

### python-port.AC7: Failure isolation across optional domains
- **python-port.AC7.1 Success:** When one optional domain raises an exception during `run()`, other optional domains continue to execute
- **python-port.AC7.2 Success:** Partial outputs of the failing domain (parquets written before the exception) remain on disk
- **python-port.AC7.3 Success:** `manifest.json` lists the failing domain with `status: "failed"` and the error message, and lists successful domains with `status: "ok"`
- **python-port.AC7.4 Success:** Exit code is 4 when at least one optional domain failed but no required domain failed
- **python-port.AC7.5 Failure:** A required domain raising an exception during `run()` halts the pipeline, exit code 3, connection closed cleanly

### python-port.AC8: Output layout and manifest
- **python-port.AC8.1 Success:** Output directory structure is exactly `<output_dir>/<dpid>/<request_id>/<table>.parquet`
- **python-port.AC8.2 Success:** `manifest.json` contains `request_id`, `dpid`, `dp_max_date`, `tool_version`, `git_sha` (if available), `started_at`, `finished_at`, `exit_code`, and per-domain `domains{}` dict
- **python-port.AC8.3 Success:** `manifest.json` per-domain entries include `status`, `outputs[]`, `row_counts{}`, and `duration_ms` for ok; `skipped_reason` for skipped; `error` and `partial_outputs[]` for failed
- **python-port.AC8.4 Success:** All 16 expected output basenames match the original SQL's COPY target names exactly

### python-port.AC9: Debug flag behaviour
- **python-port.AC9.1 Success:** With `--debug` (or `[runtime] debug = true`), `<work_dir>/scdm-snapshot-db.duckdb` persists after the run
- **python-port.AC9.2 Success:** With `--debug`, the spill directory (`temp_directory`) is not deleted
- **python-port.AC9.3 Success:** With `--debug`, intermediate `_r##_<rid>_*` tables remain in the `.duckdb` file for post-mortem inspection
- **python-port.AC9.4 Success:** Without `--debug` (default), the `.duckdb` file and spill directory are deleted after a successful run
- **python-port.AC9.5 Success:** `--debug` does not change log verbosity; `--log-level` is the separate axis for that

### python-port.AC10: DuckDB session and memory strategy
- **python-port.AC10.1 Success:** `memory_limit`, `threads`, `temp_directory`, and `preserve_insertion_order` are applied before any SELECT/COPY runs, verified via `duckdb_settings()` query
- **python-port.AC10.2 Success:** All intermediate tables are regular persistent tables, not TEMP tables (verified by presence in `duckdb_tables` after close+reopen)
- **python-port.AC10.3 Success:** File paths passed to `read_parquet(...)` are pre-resolved absolute paths and validated to live under `settings.paths.input_dir`
- **python-port.AC10.4 Failure:** A source parquet path that resolves outside `input_dir` (e.g., via symlink) is rejected at probe time

### python-port.AC11: Logging output
- **python-port.AC11.1 Success:** stdout receives one JSON object per log record, newline-delimited, parseable as jsonl
- **python-port.AC11.2 Success:** Every JSON log record contains `request_id`, `dpid`, and (when applicable) `domain` and `stage` fields
- **python-port.AC11.3 Success:** `run.log` contains the same records in human-readable format at `<output_dir>/<dpid>/<request_id>/run.log`
- **python-port.AC11.4 Success:** `--log-level DEBUG` causes SQL statements (trimmed) and exception tracebacks to appear; default INFO suppresses them

### python-port.AC12: Row-count parity with original SQL
- **python-port.AC12.1 Success:** For at least one real SCDM site, running both the original `scdm_snapshot_db.sql` and the python port with matching config produces row-count-identical parquet outputs for every shared output file
- **python-port.AC12.2 Success:** Spot-checked values (e.g., top-10 `enr_pat_covlength_md` rows, `dem_catvars_md` counts for a specific category) match between original and port
- **python-port.AC12.3 Success:** Any deviations are documented in the README's "Known Deviations" section with justification

### python-port.AC13: Dry-run and validate-config subcommands
- **python-port.AC13.1 Success:** `--dry-run` parses config, runs the probe, prints the set of domains that would execute and the set that would be skipped (with reasons), and exits 0 without writing any output parquets, run.log, or manifest.json
- **python-port.AC13.2 Success:** `validate-config` parses the config, applies all pydantic validators, and exits 0 on success or non-zero with a clear error on failure — does not probe the filesystem
- **python-port.AC13.3 Success:** `--dry-run` on a config with a missing required table still exits 2 (the probe fails before output would have been written)

## Glossary

- **SCDM**: Sentinel Common Data Model — the standardised schema used by FDA's Sentinel distributed database network for healthcare claims data.
- **DPID**: Data Partner Identifier — a short alphanumeric code (3–6 characters) uniquely identifying a participating data partner site in the Sentinel network.
- **patid**: Patient identifier — a site-local, de-identified key used to link a patient's records across SCDM tables.
- **enrollment cohort**: A derived patient set filtered to a specific coverage type; the port produces three variants (`_m` medical-only, `_d` drug-only, `_md` medical-and-drug).
- **DuckDB**: An in-process analytical database engine that executes SQL directly against parquet files; used here as both the query engine and the intermediate-table store.
- **Typer**: A Python library for building CLI applications from type-annotated function signatures; provides the `scdm-snapshot-db` entry point and its subcommands.
- **pydantic-settings**: A Pydantic extension that loads configuration from TOML files, environment variables, or programmatic overrides into validated dataclass-like models.
- **parquet**: A columnar binary file format used for both the source SCDM input tables and all output files produced by this tool.
- **window function**: A SQL construct that computes a value for each row using an ordered partition of surrounding rows; used extensively in the enrollment bridging CTEs.
- **UNPIVOT**: A SQL operation that rotates columns into rows; used in the demographic and vitals domains to normalise wide categorical tables.
- **topological sort**: An ordering of nodes in a directed acyclic graph such that each node appears before its dependents; used here to sequence domain execution by `depends_on` declarations.
- **Domain protocol**: A Python `Protocol` interface that every domain module implements, defining `name`, `is_required`, `source_tables`, `depends_on`, `outputs`, and `run()`.
- **IO probe**: The runtime component (`io_probe.py`) that checks file existence and validates column presence for every declared source table before any SQL executes.
- **manifest.json**: A per-run audit file recording request metadata, per-domain status, row counts, durations, and exit code.
- **`request_id`**: A structured string identifier for a specific analysis run, encoding the DPID, workplan number, and version suffix; validated against a regex at config load.
- **MIL**: Mother-Infant Linkage — a self-contained optional domain that computes delivery-to-infant linkage rates from the `mil` source table, independent of the enrollment cohort intermediates.

## Architecture

A single-binary Typer CLI (`scdm-snapshot-db`) loads a TOML config, opens a persistent DuckDB connection, probes the filesystem to determine which source tables exist, runs a topologically ordered set of domain modules against those tables, and writes parquet outputs plus an audit manifest to a deterministic directory layout.

The pipeline is organized as a **table-module registry**: each logical SCDM domain (enrollment, demographic, dispensing, encounter, lab, vitals, death, mil) is a separate Python module behind a uniform `Domain` protocol. A runner module discovers domains from a static registry, filters them against runtime table availability, topologically sorts by declared dependencies, and executes each domain's `run(con, ctx)` method. Failures in optional domains are isolated to that domain; failures in required domains halt the run with a non-zero exit code.

The port preserves the original SQL logic line-for-line (including all window-function CTEs, UNPIVOT constructs, and the three enrollment cohort variants `_m`/`_d`/`_md`) but replaces `SET VARIABLE`/`getvariable()` with `con.execute(sql, {params})` using `$name` placeholders, and replaces `CREATE OR REPLACE TEMP TABLE` with regular tables in a persistent `.duckdb` file under `work_dir`. This switch is load-bearing: DuckDB's buffer manager reliably pages regular tables to disk under memory pressure, while TEMP tables are memory-first and OOM before spilling on small hosts.

### Components and Boundaries

**CLI surface** (`cli.py`): Typer app with two subcommands — `run` and `validate-config`. `run` accepts `--config` plus per-field overrides (`--dpid`, `--request-id`, `--dp-max-date`, `--input-dir`, `--output-dir`, `--memory-limit`, `--threads`, `--log-level`, `--debug`, `--dry-run`).

**Config boundary** (`config.py`): Pydantic `Settings` model built from a TOML file, then merged with CLI overrides. CLI > TOML > defaults. Validation errors exit with code 2 before any work starts.

**Domain Protocol** (`domains/_base.py`): the contract every domain implements.

```python
class Domain(Protocol):
    name: str                              # canonical table name: "enrollment", "lab", ...
    is_required: bool                      # True for enrollment, demographic
    source_tables: tuple[str, ...]         # source parquet table names this reads
    depends_on: tuple[str, ...]            # other domain names that must run first
    outputs: tuple[str, ...]               # output parquet basenames (no .parquet suffix)

    def run(self, con: DuckDBPyConnection, ctx: RunContext) -> DomainResult: ...

@dataclass(frozen=True)
class OutputFile:
    domain: str
    name: str
    path: Path
    row_count: int
    bytes_written: int

@dataclass(frozen=True)
class DomainResult:
    status: Literal["ok", "skipped", "failed"]
    outputs: tuple[OutputFile, ...]
    skipped_reason: str | None = None
    error: str | None = None
    duration_ms: int = 0
```

**RunContext** (`context.py`): carries validated settings, per-table availability, resolved output directory, the runner-assigned stage index, and a logger pre-filtered with `request_id`/`dpid`/`domain` context.

```python
@dataclass(frozen=True)
class RunContext:
    settings: Settings
    available: dict[str, TableAvailability]
    output_dir: Path                       # <output_dir>/<dpid>/<request_id>/
    stage_index: int
    logger: logging.Logger
```

**IO probe** (`io_probe.py`): for each declared table, checks file existence, opens it with `read_parquet(...) LIMIT 0` to extract the column set, and validates against a hardcoded per-table `EXPECTED_COLUMNS` dict listing only the columns the port actually reads. Sites with extra columns pass; sites missing required columns are treated as unavailable.

```python
@dataclass(frozen=True)
class TableAvailability:
    name: str
    declared_in_config: bool
    file_exists: bool
    schema_ok: bool
    missing_columns: tuple[str, ...]
    resolved_path: Path | None
```

**DuckDB session** (`duckdb_session.py`): opens the connection against `<work_dir>/scdm-snapshot-db.duckdb`, applies tuning pragmas in strict order (`memory_limit`, `threads`, `temp_directory`, `preserve_insertion_order`), and returns the connection to the runner.

**Runner** (`runner.py`): orchestrates the end-to-end flow. Loads config, probes tables, filters the registry, topologically sorts, executes each domain inside try/except, collects results, writes `manifest.json`, and computes the exit code (`0`=all-ok, `2`=config/probe failure, `3`=required domain failure, `4`=one or more optional domains failed).

### Data Flow

```
TOML config + CLI flags
        |
        v
  [config.py] -> Settings (validated)
        |
        v
  [io_probe.py] -> TableAvailability per declared table
        |
        v
  [runner.py] filter + topo sort
        |
        v
  [duckdb_session.py] open .duckdb, apply pragmas
        |
        v
  for each domain in order:
        [domains/<name>.py].run(con, ctx)
        - DROP stale _r##_<rid>_* intermediates
        - CREATE intermediates via con.execute(sql, params)
        - COPY final tables TO '<output_dir>/<dpid>/<request_id>/<name>.parquet'
        - unless debug: DROP intermediates
        - return DomainResult
        |
        v
  [runner.py] write manifest.json, close connection
        |
        v
  unless debug: rm work_dir/scdm-snapshot-db.duckdb + spill dir
```

### Output Layout

```
<output_dir>/<dpid>/<request_id>/
├── enr_pat_covlength_md.parquet
├── enr_patid_ct_md.parquet
├── enr_pat_covyears_md.parquet
├── enr_pat_enrcount_md.parquet
├── enr_active_patid_ct_md.parquet
├── dem_pat_lstagecount_md.parquet
├── dem_pat_actagect_md.parquet
├── dem_catvars_md.parquet
├── dis_pat_rx_md.parquet
├── dis_pat_rx_d.parquet
├── enc_pat_enccount_md.parquet
├── lab_pat_testct_md.parquet
├── vit_pat_vitct_md.parquet
├── dth_dthct_md.parquet
├── dth_dthct_m.parquet
├── mil_linkage_rates.parquet
├── run.log
└── manifest.json
```

Intermediate tables in the persistent `.duckdb` use the naming scheme `_r##_<request_id>_<table>`, where `##` is the zero-padded stage index from the topological sort. This keeps per-stage intermediates legible when a post-mortem user opens the preserved `.duckdb` file in debug mode.

## Existing Patterns

Codebase investigation found that this repository currently contains only a single `scdm_snapshot_db.sql` file plus a README — no existing Python code, no existing module structure, no existing test harness. There are no Python patterns to follow or diverge from.

The design therefore introduces new patterns for this project:
- Typer-based CLI entry point with subcommands
- Pydantic-settings-based config with TOML loading and CLI override merging
- Protocol-based domain module registry with topological execution ordering
- Stdlib `logging` with JSON-stdout + human-file dual handler setup
- Pyarrow-synthesized parquet fixtures for hermetic testing

The SQL content itself is the one existing pattern this port strictly preserves. Every CTE, window function, UNPIVOT, and join in the python port is a direct translation of the corresponding block in `scdm_snapshot_db.sql`. Logic changes are explicitly out of scope — this is a port, not a rewrite.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Project Scaffolding and Config

**Goal:** Establish the package skeleton, build system, and config loading. After this phase the CLI exists and can validate a config file, but does no real work.

**Components:**
- `pyproject.toml` declaring `scdm-snapshot-db` package, entrypoint `scdm-snapshot-db = "scdm_snapshot_db.cli:app"`, Python `>=3.11`, runtime deps (`duckdb`, `typer`, `pydantic`, `pydantic-settings`), test deps (`pytest`, `pyarrow`)
- `src/scdm_snapshot_db/__init__.py` exposing `__version__`
- `src/scdm_snapshot_db/config.py` with `Settings`, `RequestCfg`, `PathsCfg`, `DuckDBCfg`, `LoggingCfg`, `RuntimeCfg` pydantic models; TOML loader; CLI-over-TOML-over-defaults merge function; request_id regex validator (`^[a-z0-9]+_[a-z0-9]+_wp\d+_[a-z0-9]{3,6}_[tbv]\d+$`); dpid consistency validator
- `src/scdm_snapshot_db/cli.py` with Typer app exposing `run` and `validate-config` subcommands (run is a stub at this phase)
- `src/scdm_snapshot_db/logging_setup.py` with stdlib logging configuration: JSON-formatter stdout handler + human-formatter file handler + contextvars-backed filter for `request_id`/`dpid`/`domain`/`stage`
- `example.config.toml` committed at repo root as a reference
- `tests/test_config.py` covering config precedence, request_id regex acceptance/rejection, dpid consistency check, TOML parse errors

**Dependencies:** None (first phase)

**Covers ACs:** `python-port.AC1.*`, `python-port.AC2.*`, `python-port.AC3.*`, `python-port.AC11.*`

**Done when:**
- `pip install -e .` succeeds on Python 3.11+
- `scdm-snapshot-db --help` prints both subcommands
- `scdm-snapshot-db validate-config --config example.config.toml` exits 0
- `scdm-snapshot-db validate-config --config <invalid.toml>` exits non-zero with a clear pydantic validation error
- Tests in `tests/test_config.py` pass, covering each AC listed above
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: IO Probe and DuckDB Session

**Goal:** Build the runtime detection layer and the DuckDB session manager. After this phase the CLI can inspect a site and open a tuned DuckDB connection, but no queries run yet.

**Components:**
- `src/scdm_snapshot_db/io_probe.py` with `TableAvailability` dataclass, `EXPECTED_COLUMNS: dict[str, frozenset[str]]` for every canonical table (listing only columns the port reads), `probe(settings) -> dict[str, TableAvailability]` function
- `src/scdm_snapshot_db/duckdb_session.py` with `open_session(settings) -> DuckDBPyConnection` function that opens the persistent `.duckdb` file at `<work_dir>/scdm-snapshot-db.duckdb`, applies tuning pragmas in the documented order, and returns the connection
- `src/scdm_snapshot_db/context.py` with `RunContext` dataclass
- `tests/test_io_probe.py` with fixture builders that synthesize minimal parquet files via `pyarrow`, covering: all-tables-present, optional-missing, required-missing, schema-drift (column present in expected but absent in fixture), path-escape rejection (resolved path outside `input_dir`)
- `tests/fixtures/builders.py` with `make_scdm_site(tmp_path, tables={...}) -> Path` helper that writes tiny parquet files matching the EXPECTED_COLUMNS shape

**Dependencies:** Phase 1 (config types)

**Covers ACs:** `python-port.AC4.*`, `python-port.AC5.*`, `python-port.AC10.*`

**Done when:**
- `io_probe.probe(settings)` correctly classifies every test fixture scenario
- Path-escape attempts (symlinks or relative paths pointing outside `input_dir`) are rejected
- `open_session` produces a connection with the expected pragmas set (verified by querying `duckdb_settings()`)
- Tests pass covering each AC listed above
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Runner, Domain Protocol, and Enrollment Domain

**Goal:** Establish the runner orchestration and the first (required, most complex) domain. After this phase the CLI can run enrollment end-to-end against a fixture site.

**Components:**
- `src/scdm_snapshot_db/domains/_base.py` with `Domain` Protocol, `OutputFile` and `DomainResult` dataclasses
- `src/scdm_snapshot_db/domains/__init__.py` with `DOMAIN_REGISTRY` mapping domain names to module instances, initially containing only `enrollment`
- `src/scdm_snapshot_db/runner.py` with `Runner.run(settings) -> ExitCode` method handling: probe call, registry filter (required-missing fails, optional-missing skipped), topological sort by `depends_on`, per-domain try/except, intermediate table cleanup unless debug, manifest.json write, work-dir cleanup unless debug
- `src/scdm_snapshot_db/domains/enrollment.py` porting the three enrollment cohort CTEs (`enr_final_m`, `enr_final_d`, `enr_final_md`), the patid distinct sets, `LengthOfEnrollment`, and all 5 enrollment output parquets; intermediates named `_r01_<request_id>_*`; optional chunking path gated on `settings.duckdb.enrollment.chunked`
- `tests/test_runner.py` with happy-path, required-missing (exit 2), optional-missing (continues), and debug flag tests
- `tests/domains/test_enrollment.py` asserting each of the 5 enrollment outputs against hand-crafted fixture data with known expected row counts and spot values

**Dependencies:** Phase 2 (io_probe, session)

**Covers ACs:** `python-port.AC5.*`, `python-port.AC6.*`, `python-port.AC7.*`, `python-port.AC8.*`, `python-port.AC9.*`

**Done when:**
- `scdm-snapshot-db run --config <fixture.toml>` against an enrollment-only fixture site produces all 5 enrollment parquets at the expected paths
- Required-missing scenario exits with code 2 and writes no parquets
- Optional-missing scenario skips the optional domain with a warning log and continues
- `--debug` preserves `<work_dir>/scdm-snapshot-db.duckdb`; without it, the file is deleted
- `manifest.json` is written with correct domain statuses, row counts, and timestamps
- Chunked enrollment path (opt-in via config) produces row-count-identical output to the non-chunked path on the fixture site
- Tests pass covering each AC listed above
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Demographic Domain and Required-Domain Gating

**Goal:** Port the second required domain and prove the two-required-domain fail-fast behaviour end-to-end.

**Components:**
- `src/scdm_snapshot_db/domains/demographic.py` porting `dem_age_md`, `dem_pat_lstagecount_md`, `dem_pat_actagect_md`, and `dem_catvars_md` (including UNPIVOT); depends on enrollment's `enr_final_md` and `enr_patid_md` intermediates; `is_required=True`
- Registry update in `domains/__init__.py` to include demographic
- `tests/domains/test_demographic.py` asserting all 3 demographic outputs against fixture data with known age distributions and UNPIVOT expansion
- Integration test in `tests/test_runner.py` verifying: (a) missing demographic fails fast with exit 2 even when enrollment is present, (b) both-required-present produces all 8 enrollment+demographic parquets

**Dependencies:** Phase 3 (runner, enrollment)

**Covers ACs:** `python-port.AC4.*`, `python-port.AC6.*`

**Done when:**
- All 3 demographic parquets produced against fixture site
- Missing demographic exits 2, even with enrollment present
- All AC-level tests pass
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Optional Domains — Dispensing, Encounter, Lab, Death

**Goal:** Port the four optional enrollment-dependent domains. These share the same structural pattern (join source table against enrollment cohort intermediate, aggregate, write parquet).

**Components:**
- `src/scdm_snapshot_db/domains/dispensing.py` porting `dis_pat_rx_ct`, `dis_pat_rx_md`, `dis_pat_rx_d`; depends on enrollment; 2 outputs
- `src/scdm_snapshot_db/domains/encounter.py` porting `enc_pat_enccount_md`; depends on enrollment; 1 output
- `src/scdm_snapshot_db/domains/lab.py` porting `lab_pat_testct_md`; depends on enrollment; 1 output
- `src/scdm_snapshot_db/domains/death.py` porting `dth_dthct_md` and `dth_dthct_m`; depends on enrollment (uses both `enr_patid_m` and `enr_patid_md`); 2 outputs
- Registry update in `domains/__init__.py` to include all four
- One test file per domain in `tests/domains/` asserting output shape and row counts against fixture data
- `tests/test_runner.py` case verifying failure isolation: a mocked exception in lab leaves dispensing/encounter/death outputs intact, lab's partial outputs preserved, exit code 4, manifest reflects `status: "failed"` for lab and `status: "ok"` for the rest

**Dependencies:** Phase 4 (full required-domain set operational)

**Covers ACs:** `python-port.AC5.*`, `python-port.AC7.*`, `python-port.AC8.*`

**Done when:**
- All 6 optional-enrollment-dependent parquets produced against fixture site
- Failure isolation test passes (one domain fails, others continue, exit 4)
- Partial outputs from a failed domain remain on disk and are listed in `manifest.json`
- Tests pass covering each AC listed above
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Vitals and MIL Domains

**Goal:** Port the last two optional domains, including the resurrected vitals block and the self-contained MIL domain.

**Components:**
- `src/scdm_snapshot_db/domains/vitals.py` porting the commented-out vitals block (`vit_pat_vitct_md`); depends on enrollment; uses UNPIVOT for HGT/WGT/DIA/SYS; the original `ALTER TABLE ... DROP sortOrder` is folded into the SELECT list ordering so the COPY-to-parquet step does not need a follow-up
- `src/scdm_snapshot_db/domains/mil.py` porting `mil_linkage_rates`; no dependencies on enrollment (self-contained); validates the protocol's handling of a dep-free optional domain
- Registry update to include both
- `tests/domains/test_vitals.py` and `tests/domains/test_mil.py` with fixture data covering the UNPIVOT expansion and the MIL linkage-rate aggregations
- Full-site integration test in `tests/test_runner.py`: fixture with all 8 declared tables present → runner produces all 16 expected parquets, manifest shows all domains ok, exit 0

**Dependencies:** Phase 5 (runner proven with multiple optional domains)

**Covers ACs:** `python-port.AC5.*`, `python-port.AC8.*`

**Done when:**
- Vitals and MIL parquets produced against fixture site
- Full happy-path test produces all 16 parquets
- `manifest.json` lists all 8 domains as `ok`
- Tests pass covering each AC listed above
<!-- END_PHASE_6 -->

<!-- START_PHASE_7 -->
### Phase 7: Real-Site Validation and Documentation

**Goal:** Validate the port against a real site's output and produce the operator-facing documentation.

**Components:**
- Manual validation run: pick one real SCDM site the user has access to, run both the original `scdm_snapshot_db.sql` and the new python CLI against it with matching config (same `dpid`, `dp_max_date`, `input_dir`), diff the parquet outputs via a small comparison script that reports row counts and spot-checks specific values
- `scripts/compare_outputs.py` helper that takes two output directories and reports per-file row counts, column sets, and a small sample comparison
- Documented deviations (if any) in the design plan's Additional Considerations section or in the README's "Known Deviations from Original SQL" section
- `README.md` rewritten to cover: install (pip + optional uv), config reference with every field explained, CLI usage examples, output layout, the enrollment OOM playbook (`chunked=true`), debug mode, manifest.json schema summary
- Original `scdm_snapshot_db.sql` retained at the repo root untouched

**Dependencies:** Phase 6 (all domains operational)

**Covers ACs:** `python-port.AC12.*`, `python-port.AC13.*`

**Done when:**
- Row-count-identical output between python port and original SQL on at least one real site (spot-checked values match)
- Any deviations documented in the README
- README sections complete: install, config, usage, output, playbook, debug, manifest
- Original `scdm_snapshot_db.sql` still present at repo root
<!-- END_PHASE_7 -->

## Additional Considerations

**Error handling:** Config/probe failures exit 2 before any DuckDB work begins. Required-domain failures exit 3 and halt the pipeline; the connection is closed cleanly and partial enrollment/demographic intermediates are dropped unless `--debug`. Optional-domain failures are isolated: the domain's exception is caught, logged at `ERROR` with traceback at `DEBUG` level, the domain's partial outputs remain on disk, and the runner continues to the next domain. If one or more optional domains fail, the final exit code is 4; if all succeed, it is 0. The four exit codes (0/2/3/4) are documented in the README.

**Enrollment OOM playbook:** The enrollment bridging CTEs use three stacked window functions partitioned by `patid`, across three cohort variants. On memory-constrained hosts (≤8GB RAM, site with tens of millions of patids) this can OOM even with `memory_limit` set, per DuckDB issue #14132. The default execution path is unchunked. If an operator sees an OOM log from the enrollment domain, the documented recovery is to set `[duckdb.enrollment] chunked = true` and `chunks = 16` (or higher) in the config and re-run. The chunked path hash-buckets patids and runs the window query per chunk. This is intentionally operator-in-the-loop rather than auto-detected.

**Memory strategy deviation from prototype:** The original SQL uses `CREATE OR REPLACE TEMP TABLE` exclusively. This port replaces every temp table with a regular persistent-file table prefixed `_r##_<request_id>_`. This is the single most important change for small-host viability and is not optional. Intermediate tables are dropped at the end of each domain's `run()` unless `debug=true`.

**Manifest as future config input:** A near-term future enhancement may use the `manifest.json` produced by an upstream program as the config for a downstream run of this tool. The design intentionally keeps `manifest.json` close in shape to the `Settings` model (field names and nesting align) so that a future loader can consume it with minimal adapter logic. This work is **out of scope** for this port; it is noted here so the field naming decisions are understood as forward-looking.

**Out of scope for this port, intentionally:**
- Retry logic on any domain failure
- Parallel domain execution (DuckDB connection is single-writer; parallelism is marginal gain for this workload)
- Resumability from a previous partial run (re-run from scratch only)
- Output parquet schema versioning or migration
- Diagnosis, procedure, facility, and provider domains (registry architecture supports them as drop-in additions but they are not part of this port)
- Auto-detection of chunked enrollment fallback
- Vitals chunking mitigation (UNPIVOT is not as OOM-prone as stacked window functions)

