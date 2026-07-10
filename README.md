# SCDM Snapshot DB — PySpark Edition

A PySpark-based analytical pipeline that replaces the original DuckDB SQL workload
(`scdm_snapshot_db.sql`) with an installable Python package executing in local Spark
mode. It applies **corrected analytical semantics** and writes 15 active logical
outputs as Spark-native Parquet datasets.

> **Corrected contract is authoritative.** This implementation intentionally differs
> from the source SQL in several places (documented below). Do not assume
> byte-for-byte equivalence with historical SAS or DuckDB output.

## Prerequisites

- **Python 3.11+**
- **Java 8, 11, or 17** (required by PySpark 3.5; Java 8 is the minimum)
- **Disk/memory:** At least 4 GB driver memory for moderate datasets; adjust per your
  machine and input size.

## Installation

### Option A: Using `uv` (recommended for development)

```bash
cd /path/to/scdm_snapshot_db
uv venv --python python3.11 .venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Option B: Using `python -m pip` (for restricted environments)

```bash
cd /path/to/scdm_snapshot_db
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

> **Never use bare `pip`** when `python -m pip` is safer; the latter ensures you
> install into the correct interpreter.

Verify the installation:

```bash
scdm-snapshot --help
```

You should see `run`, `benchmark`, and `compare` subcommands.

## Configuration

Create a TOML configuration file (see `example_config.toml`):

```toml
[request]
dpid = "MKSCNR"
dp_max_date = "2023-06-30"

[inputs]
enrollment = "/path/to/enrollment.parquet"
demographic = "/path/to/demographic.parquet"
# ... other domains as needed

[output]
root = "output"
domains = ["enrollment", "demographic", "dispensing", "encounter", "lab", "death", "mil"]

[spark]
master = "local[*]"
driver_memory = "4g"
shuffle_partitions = 200

[write]
mode = "errorifexists"
```

### Configuration Reference

| Section | Key | Type | Default | Description |
|---|---|---|---|---|
| `[request]` | `dpid` | string | (required) | Data partner identifier |
| `[request]` | `dp_max_date` | date | `2023-06-30` | Reference date for active-patient logic |
| `[inputs]` | (domain names) | string | — | Logical paths to Parquet datasets |
| `[output]` | `root` | string | `"output"` | Output directory root |
| `[output]` | `domains` | list | (required) | Selected domains |
| `[spark]` | `master` | string | `local[*]` | Spark master URL (local only) |
| `[spark]` | `driver_memory` | string | `4g` | Driver memory |
| `[spark]` | `shuffle_partitions` | int | `200` | Shuffle partition count |
| `[spark]` | `adaptive_query_execution` | bool | `true` | Enable AQE |
| `[spark]` | `session_timezone` | string | `UTC` | Timezone for date operations |
| `[spark]` | `broadcast_strategy` | string | `auto` | `auto`, `broadcast`, or `disabled` |
| `[spark]` | `output_partitions` | int | `0` | Output partition count (0=auto) |
| `[write]` | `mode` | string | `errorifexists` | `errorifexists`, `overwrite`, or `ignore` |
| `[benchmark]` | `repetitions` | int | `5` | Benchmark repetitions |
| `[benchmark]` | `warmup_repetitions` | int | `0` | Discarded warm-up runs |

## Domain Selection and Dependencies

| Domain | Requires Enrollment | Sub-Cohorts | Source Input |
|---|---|---|---|
| enrollment | (is enrollment) | m, md, d | enrollment |
| demographic | yes | md | demographic |
| dispensing | yes | md, d | dispensing |
| encounter | yes | md | encounter |
| lab | yes | md | lab |
| death | yes | md, m | death |
| mil | no | — | mil |

- Selecting a dependent domain **automatically includes enrollment**.
- Only the input paths for selected domains need to exist and be valid.
- MIL can run independently without any enrollment data.

## Usage

### Run the pipeline

```bash
scdm-snapshot run --config my_config.toml
```

CLI overrides:

```bash
scdm-snapshot run --config my_config.toml --output-root /tmp/my_run --dpid OTHER
```

### Benchmark

```bash
scdm-snapshot benchmark --config my_config.toml
```

Each repetition runs in a fresh Python/JVM subprocess to avoid session/cache
contamination. Output directories are isolated per repetition.

### Compare outputs against SAS

```bash
scdm-snapshot compare \
  --config my_config.toml \
  --actual-root output \
  --reference-root /path/to/sas_outputs \
  --numeric-tolerance 0.01
```

## Output Layout

Each logical output is written to `<output_root>/<output_name>/` as a standard
Spark Parquet dataset directory. Outputs are **not** forced to a single part file.

### The 15 active outputs

| Output Name | Domain | Description |
|---|---|---|
| `enr_pat_covlength_md` | enrollment | Inclusive span length distribution |
| `enr_patid_ct_md` | enrollment | Distinct md patient count |
| `enr_pat_covyears_md` | enrollment | Distinct patient-year coverage |
| `enr_pat_enrcount_md` | enrollment | Bridged span count distribution |
| `enr_active_patid_ct_md` | enrollment | Distinct active patient count |
| `dem_pat_lstagecount_md` | demographic | Latest-stage age category counts |
| `dem_pat_actagect_md` | demographic | Active patient age category counts |
| `dem_catvars_md` | demographic | Categorical variable counts |
| `dis_pat_rx_md` | dispensing | Dispensing count distribution (md) |
| `dis_pat_rx_d` | dispensing | Dispensing count distribution (d) |
| `enc_pat_enccount_md` | encounter | Encounter count distribution |
| `lab_pat_testct_md` | lab | Lab test count distribution |
| `dth_dthct_md` | death | Death count (md cohort) |
| `dth_dthct_m` | death | Death count (m cohort) |
| `mil_linkage_rates` | mil | Maternal-infant linkage rates |

The commented **vitals** block from the source SQL is intentionally absent.

## Analytical Contract and Deviations from `scdm_snapshot_db.sql`

The following corrections are **intentional** and authoritative. The output may
differ from historical SAS/DuckDB results where these corrections apply.

1. **Robust running-maximum interval bridging:** Replaces the source's `LAG(enr_end)`
   with a running maximum of `enr_end` over all preceding rows. This correctly handles
   nested intervals where a short interval appears between longer ones.

2. **Deterministic ties:** Input ordering does not affect output; intervals are sorted
   by `(enr_start, enr_end)` before bridging.

3. **Inclusive durations:** Span length is `datediff(end, start) + 1`, not
   `datediff(end, start)`.

4. **Distinct patient-year coverage:** Each patient is counted at most once per
   calendar year (deduplicated `(patid, year)`), not once per span-year explosion.

5. **Distinct active-patient counting:** Uses a direct `distinct` count of patients
   with any bridged span ending on or after `dp_max_date`.

6. **Deterministic latest-stage demographics:** Replaces the source's ineffective
   `last_value(patid)` with `row_number()` selection of the span with maximum
   `_enr_start` (tie-break: max `_enr_end`).

7. **Exhaustive demographic age bands:** Age 80 is included in `80+ yrs` (source
   uses `> 80`, missing exactly 80). All categories are exhaustive.

8. **Retained null-birth patients:** Patients with null `birth_date` are retained as
   `MISSING` rather than dropped.

9. **Exhaustive MIL age bands:** Extends source's 10-54-only range to cover all ages
   (0-1 through 55+) plus `MISSING` and `NEGATIVE`.

10. **Numeric MIL measures:** Deliveries, linked deliveries, and distinct infants are
    stored as long integers; linkage rate is `decimal(9,6)`. No commas, percent signs,
    or presentation formatting in Parquet data.

11. **Stable naming and types:** All outputs use consistent lowercase column names and
    explicit Spark types regardless of source casing.

12. **Lab `test_dt` filtering:** Applied to the computed `COALESCE(lab_dt, result_dt,
    order_dt)` result, not any same-named source column.

## Fair SAS-Comparison Methodology

A valid "faster than SAS" conclusion requires:

- **Same source Parquets** for both systems.
- **Same selected domains** and `dp_max_date`.
- **Fresh output directories** for each run (no reuse).
- **Multiple fresh-process repetitions** (not single runs).
- **No other major workload** on the machine during measurement.
- **Median plus range** (min/max/dispersion) reported, not a single value.
- **Complete benchmark JSON** preserved for reproducibility.

> **Benchmark speed does not imply output equivalence.** The corrected contract
> intentionally changes results. Use the `compare` command to verify specific
> outputs when equivalence is needed.

## Run-Result Metadata

Every run produces a machine-readable JSON file containing:

```json
{
  "success": true,
  "elapsed_seconds": 12.34,
  "selected_domains": ["enrollment", "demographic", ...],
  "selected_outputs": ["enr_pat_covlength_md", ...],
  "python_version": "3.11.14",
  "pyspark_version": "3.5.8",
  "effective_spark_settings": {...},
  "started_at": "2025-07-10T...",
  "finished_at": "2025-07-10T..."
}
```

This does not trigger extra Spark jobs for metadata.

## Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Pipeline execution failure |
| 2 | Configuration error (before Spark starts) |

## Local Spark Tuning

Tuning depends on cores, RAM, input cardinality, skew, and disk. Key controls:

- **`spark.shuffle.partitions`**: Start with 200; increase for large data.
- **`spark.driver.memory`**: Must fit your data and broadcast tables.
- **`adaptive_query_execution`**: Enabled by default; recommended.
- **`broadcast_strategy`**: `auto` (default) lets Spark decide; `broadcast` forces
  broadcast hash join on enrollment spans; `disabled` turns off broadcast. Benchmark
  all three on your target machine before changing the default.
- **`output_partitions`**: 0 (default) lets Spark decide; set >0 to control write
  parallelism (changes the measured write plan).

Do not globalize a one-machine result as a universal default.

## Development

```bash
# Fast unit tests (no Spark needed)
python -m pytest -m "not integration"

# Full test suite (requires Java + Spark)
python -m pytest

# Linting
ruff format --check .
ruff check .

# Type checking
mypy --strict src tests
```

## Architecture

The package follows a **functional-core/imperative-shell** structure adapted to
PySpark:

- **Functional core** (`config_models`, `config_validation`, `schema_contracts`,
  `error_classification`, `models`, `transforms/`): Pure functions that construct
  DataFrame plans from input DataFrames and scalar configuration. No IO, no Spark
  sessions, no actions.

- **Imperative shell** (`config_loading`, `input_validation`, `spark_session`,
  `inputs`, `outputs`, `pipeline`, `benchmark`, `logging_setup`, `cli`): Parses
  config, creates/stops sessions, reads/writes files, coordinates plan execution.

Every source file begins with `# pattern: Functional Core` or
`# pattern: Imperative Shell`.


## Historical Reference

`scdm_snapshot_db.sql` is retained as the historical workload specification. The
PySpark package is a greenfield replacement, not a port.
