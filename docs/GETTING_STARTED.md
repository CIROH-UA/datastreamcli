# Getting Started with DataStreamCLI

This document is a hands-on introduction to `DataStreamCLI`. It builds up one concrete command at a time: we start with the bare minimum required to run a NextGen simulation, then add a single new argument in each example and explain what it does. Along the way you'll see every argument, the Docker image environment variables, a full retrospective example, and a worked example that reproduces a NextGen Research DataStream (NRDS) simulation.

**The examples are meant to be run in order.** They all share the same small test domain — the Provo River near Woodland, Utah — which you'll download in [Step 1](#step-1--get-a-hydrofabric-geopackage). Each example writes to its own output directory, and later examples reuse files produced by earlier ones. If you copy them top to bottom, they should all succeed. The one exception [Example 11](#example-11--warm-start-t-route-with-a-restart-file--t--w) is a template that needs restart files for your own domain.
 
> **How this relates to the interactive guide.** The [`datastream_guide`](../scripts/datastream_guide) script is an *interactive* tour that asks you questions and assembles a command for you. This document covers the same concepts through *fixed, explained examples* you can copy, run, and modify. 

## Contents
- [What DataStreamCLI does](#what-datastreamcli-does)
- [Before you begin](#before-you-begin)
- [Step 1 — Get a hydrofabric geopackage](#step-1--get-a-hydrofabric-geopackage)
- [The anatomy of a command: the required arguments](#the-anatomy-of-a-command-the-required-arguments)
- [Example 1 — The minimal run](#example-1--the-minimal-run)
- [Example 2 — Controlling compute with `-n`](#example-2--controlling-compute-with--n)
- [Example 3 — Name the domain (`-D`)](#example-3--name-the-domain--d)
- [Example 4 — Preview a run without computing (`-y`, `-V`)](#example-4--preview-a-run-without-computing--y--v)
- [Example 5 — Reuse files with a resource directory (`-r`)](#example-5--reuse-files-with-a-resource-directory--r)
- [Example 6 — Bring your own inputs (`-F`, `-f`, `-N`)](#example-6--bring-your-own-inputs--f--f--n)
- [Example 7 — A retrospective simulation, in depth (`-C`)](#example-7--a-retrospective-simulation-in-depth--c)
- [Example 8 — Operational forecasts and `DAILY` mode](#example-8--operational-forecasts-and-daily-mode)
- [Example 9 — Reproduce a NextGen Research DataStream (NRDS) simulation (`-c`)](#example-9--reproduce-a-nextgen-research-datastream-nrds-simulation--c)
- [Example 10 — Run an LSTM ensemble (`-L`)](#example-10--run-an-lstm-ensemble--l)
- [Example 11 — Warm-start t-route with a restart file (`-t`, `-w`)](#example-11--warm-start-t-route-with-a-restart-file--t--w)
- [Docker image versions and environment variables](#docker-image-versions-and-environment-variables)
- [Where to go next](#where-to-go-next)

---

## What DataStreamCLI does

`DataStreamCLI` is a single command-line tool that automates the entire workflow of running a [NextGen](https://github.com/NOAA-OWP/ngen) water-model simulation:

1. **Collects the spatial domain** — a hydrofabric geopackage you provide with `-g` (see [Step 1](#step-1--get-a-hydrofabric-geopackage)).
2. **Builds the forcings** — it turns gridded National Water Model forcings into per-catchment NextGen forcings with [forcingprocessor](https://github.com/CIROH-UA/forcingprocessor).
3. **Generates the NextGen model configuration files** — the per-catchment BMI config files implied by your realization file.
4. **Validates** the assembled input package.
5. **Runs NextGen** through [NextGen In A Box](https://github.com/CIROH-UA/NGIAB-CloudInfra) (NGIAB).
6. **Handles outputs** — converts t-route output, versions everything (so a run is reproducible), and optionally uploads to S3.

Each step runs inside a Docker container, so most of what you install is `docker` itself. The design philosophy is **"batteries included, but flexible"**: DataStreamCLI builds every input for you by default, but you can hand it your own forcings, BMI configs, or hydrofabric at any step.

---

## Before you begin

**Install the prerequisites** in [INSTALL.md](../INSTALL.md): `docker`, `git`, `pigz`, `tar`, and `awscli` (only for the S3 options).

**Clone the repository** and work from its root folder — every command below assumes that:
```bash
git clone https://github.com/CIROH-UA/datastreamcli.git
cd datastreamcli
```

Three things to keep in mind:

- **`DATA_DIR` must not already exist.** DataStreamCLI refuses to overwrite an output directory. If it exists, delete it or choose a new path. This is deliberate — it prevents you from silently clobbering a previous run.
- **Use absolute paths** (e.g. `$(pwd)/data/my_run`). The workflow mounts these directories into Docker containers, which require absolute paths.
- **All dates are UTC**, in `YYYYMMDDHHMM` format (or the literal string `DAILY`, covered in [Example 8](#example-8--operational-forecasts-and-daily-mode)).

---

## Step 1 — Get a hydrofabric geopackage

For this guide we'll use the geopackage from the **AWI sample data package** — the same NextGen input package referenced by [NGIAB-CloudInfra](https://github.com/CIROH-UA/NGIAB-CloudInfra), covering the Provo River near Woodland, Utah (USGS gage 10154200). 

```bash
curl -LO https://ciroh-ua-ngen-data.s3.us-east-2.amazonaws.com/AWI-009/AWI_16_10154200_009.tar.gz
tar -xzf AWI_16_10154200_009.tar.gz
cp AWI_16_10154200_009/config/gage-10154200_subset.gpkg .
```

The package is a complete NextGen run directory, so it also contains forcings, a realization, and BMI config files. You can ignore all of that here — the whole point of DataStreamCLI is that it generates those for you from the geopackage and a realization. (Later, [Example 6](#example-6--bring-your-own-inputs--f--f--n) shows how to supply  files yourself when you want to.)

### Making a geopackage for your own domain

To model a different domain, use your own geopackage or create one with [**NGIAB Data Preprocess**](https://github.com/CIROH-UA/NGIAB_data_preprocess).

Complete v2.2 geopackages for each VPU are also published under `resources/v2.2_hydrofabric/geopackages/` in the [Research DataStream bucket](https://datastream.ciroh.org) if you want to run a whole basin.

---

## The anatomy of a command: the required arguments

You run the tool with the [`scripts/datastream`](../scripts/datastream) shell script. `./scripts/datastream -h` prints every option, but a basic run needs only these:

| Flag | Long name | What it defines |
|------|-----------|-----------------|
| `-s` | `--START_DATE` | Start of the simulation, `YYYYMMDDHHMM` (UTC) or `DAILY`. |
| `-e` | `--END_DATE` | End of the simulation, `YYYYMMDDHHMM` (UTC). Required unless `START_DATE` is `DAILY`. |
| `-C` | `--FORCING_SOURCE` | Where the forcings come from (which NWM product). Defaults to `NWM_RETRO_V3`. |
| `-d` | `--DATA_DIR` | Absolute local path where the run is built. Must not already exist. |
| `-g` | `--GEOPACKAGE` | Hydrofabric file defining the spatial domain. *(Or supply it via a resource directory.)* |
| `-R` | `--REALIZATION` | The NextGen realization file — the scientific configuration. *(Or supply it via a resource directory.)* |

Those six answer the three fundamental questions of any simulation — **when** (`-s`, `-e`, `-C`), **where** (`-g`), and **what science** (`-R`) — plus **where to write** (`-d`). Every other argument in this guide is optional and layers extra behavior on top.

> **The realization file is where the science lives.** It specifies which BMI models run (CFE, PET, Noah-OWP-Modular, SLoTH, t-route, LSTM, …) and their parameters. This repo ships ready-to-use templates in [`configs/ngen/`](../configs/ngen); see [NGEN_MODELS.md](NGEN_MODELS.md) for the models DataStreamCLI can auto-configure. As you go further, you'll edit this file to encode your own science.

---

## Example 1 — The minimal run

The smallest useful command. It runs a 24-hour **retrospective** NextGen simulation over the Provo River domain from [Step 1](#step-1--get-a-hydrofabric-geopackage), using one of the repo's realization templates (CFE + SLoTH + PET + Noah-OWP-Modular + t-route).

```bash
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_intro \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json
```

Argument by argument:

- **`-s 202006200100`** — start at 01:00 UTC on 2020-06-20. (Hourly NWM forcings conventionally start at hour 01.)
- **`-e 202006210000`** — end at 00:00 UTC on 2020-06-21, i.e. 24 hourly steps.
- **`-C NWM_RETRO_V3`** — use National Water Model **v3 Retrospective** forcings. "Retrospective" means a reanalysis dataset covering the past; it's the right choice for studying historical periods. (`NWM_RETRO_V2` is the older v2 reanalysis.)
- **`-d $(pwd)/data/provo_intro`** — build everything here. This path must not exist yet.
- **`-g $(pwd)/gage-10154200_subset.gpkg`** — the hydrofabric you downloaded in Step 1.
- **`-R …/realization_sloth_nom_cfe_pet_troute.json`** — a realization template from this repo.

**A note on paths.** Both `-g` and `-R` are **local files** here, but they don't have to be. DataStreamCLI resolves every file argument wherever it lives — a local path, an `https://` URL, or an `s3://` URI (the last requires `awscli` configured with credentials). That flexibility is what lets you reproduce someone else's run by pasting their URLs, without downloading anything first; you'll see it used in [Example 9](#example-9--reproduce-a-nextgen-research-datastream-nrds-simulation--c).

When it finishes, your NextGen outputs are at `$(pwd)/data/provo_intro/ngen-run/outputs/`. Everything below adds one capability at a time.

---

## Example 2 — Controlling compute with `-n`

NextGen and forcingprocessor parallelize across processes. Use `-n` / `--NPROCS` to cap how many they use.

```bash
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_n4 \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json \
  -n 4
```

- **`-n 4`**  If you omit `-n`, DataStreamCLI uses **4** by default.

Why you care: bigger domains and longer time periods need more memory, and each process consumes RAM. On a dedicated host you can safely raise `-n` toward the core count minus one; on a shared machine, keep it low so you don't starve other work. [USAGE.md](USAGE.md) gives rules of thumb (roughly 1–4 GB RAM per process depending on run length, and a runtime of about 1 minute per 10 simulated time steps). If a run crashes, the first thing to try is fewer processes or a shorter time window.

---

## Example 3 — Name the domain (`-D`)

By default DataStreamCLI derives a domain name from the geopackage filename. Set it explicitly with `-D` when you want a clean label in the run metadata.

```bash
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_named \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json \
  -D PROVO_RIVER_WOODLAND \
  -n 4
```

- **`-D PROVO_RIVER_WOODLAND`** (`--DOMAIN_NAME`) — a human-readable label for the spatial domain. It's recorded in the run metadata and helps you tell runs apart later. Omit it and DataStreamCLI uses the geopackage's base filename (here, `gage-10154200_subset`). Adding this option does not impact the processing in any way.

---

## Example 4 — Preview a run without computing (`-y`, `-V`)

Before committing to a long run, you can do a dry run: DataStreamCLI sets up the directories and **prints the Docker commands it would execute** instead of running the heavy compute steps.

```bash
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_dryrun \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json \
  -y True \
  -V True
```

- **`-y True`** (`--DRYRUN`) — skip the compute steps (forcingprocessor, BMI config generation, validation, NextGen, …). The metadata and configuration files are still generated, and the commands that *would* have run are echoed. Great for sanity-checking paths and understanding the pipeline.
- **`-V True`** (`--VERBOSE`) — stream the full output of forcingprocessor and NGIAB to your terminal instead of keeping it quiet. Useful for debugging.

Tip: after a dry run, look inside `data/provo_dryrun/datastream-metadata/` to see the generated `conf_fp.json`, `conf_nwmurl.json`, and `datastream_steps.txt`.

---

## Example 5 — Reuse files with a resource directory (`-r`)

Every run produces a `datastream-resources/` folder — a cache of the (relatively expensive) inputs it built: the geopackage, the realization, the generated BMI configs, and the NextGen forcings. Feed that folder back in with `-r` and DataStreamCLI runs in **"lite mode"**, reusing whatever it finds instead of recomputing or re-downloading it.

**Step 1 — copy the resources out of the Example 1 run:**
```bash
cp -r $(pwd)/data/provo_intro/datastream-resources $(pwd)/data/provo_resources
```

**Step 2 — rerun using the cache.** Notice `-g` and `-R` are gone: DataStreamCLI finds them inside the resource directory.
```bash
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_reuse \
  -r $(pwd)/data/provo_resources \
  -n 4
```

- **`-r $(pwd)/data/provo_resources`** (`--RESOURCE_DIR`) — read cached inputs from here. Anything present is reused; anything missing is regenerated from your other arguments.

`-r` accepts a **folder-like** path: either a local directory (as here) or an `s3://` URI *prefix*, which DataStreamCLI treats like a directory and pulls the cached files from. (This mirrors the file-like paths from Example 1, but for whole directories — it's how the Research DataStream shares a common cache across many machines.)

This is the key to **fast, reproducible iteration**, and it's exactly how the NextGen Research DataStream runs hundreds of simulations a day. Two common patterns:

- **Change only the science.** Keep the resources but delete the realization (and the `config/ngen-bmi-configs.tar.gz`, which is tied to the old realization). Pass a new `-R`; the geopackage and forcings are reused. This makes A/B-testing model configurations cheap.
- **Change only the time.** Delete `ngen-forcings/` from the resource directory (forcings are time-dependent) but keep the geopackage and BMI configs. Rerun with new `-s`/`-e`.

> **Validation guards this.** If you reuse forcings from the wrong time period, the built-in validator catches the mismatch and stops with an explanatory error rather than producing bad science. See the resource-directory layout in [STANDARD_DIRECTORIES.md](STANDARD_DIRECTORIES.md#resource_dir-datastream-resources).

---

## Example 6 — Bring your own inputs (`-F`, `-f`, `-N`)

"Batteries included, but flexible" means you can override any input DataStreamCLI would otherwise build. This example supplies ready-made NextGen forcings and skips forcingprocessor entirely.

**Step 1 — grab the forcings produced by the Example 1 run** (they already match our time window):
```bash
cp $(pwd)/data/provo_intro/datastream-resources/ngen-forcings/*.nc $(pwd)/provo_forcings.nc
```

**Step 2 — hand them to DataStreamCLI with `-F`:**
```bash
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_byo \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json \
  -F $(pwd)/provo_forcings.nc \
  -n 4
```

- **`-F` (`--NGEN_FORCINGS`)** — supply ready-made NextGen (catchment-averaged) forcings and skip forcingprocessor. Accepts a NetCDF `.nc` (as here), a `.tar.gz`, or a directory. Use this when you have your own forcing pipeline or a non-NWM forcing source. Because our supplied forcings cover the same window as `-s`/`-e`, validation passes; if they didn't, DataStreamCLI would stop and tell you.

The same "bring your own" idea applies to two more arguments (not shown as standalone commands here, since they need files specific to your setup):

- **`-f` (`--NWM_FORCINGS_DIR`)** — point at a local directory of *gridded* NWM NetCDF files. DataStreamCLI still runs forcingprocessor but reads these local files instead of downloading them — handy when you already have the NWM grids and want to avoid network transfer.
- **`-N` (`--NGEN_BMI_CONFS`)** — supply your own per-catchment BMI configuration files (directory or tarball) instead of generating them from the realization. The AWI package from [Step 1](#step-1--get-a-hydrofabric-geopackage) contains exactly this kind of folder at `AWI_16_10154200_009/config/cat_config/`.

Each of these can also live inside a resource directory (`ngen-forcings/`, `nwm-forcings/`, `config/cat-config/`) — see [STANDARD_DIRECTORIES.md](STANDARD_DIRECTORIES.md).

---

## Example 7 — A retrospective simulation, in depth (`-C`)

Example 1 was already retrospective; here we treat it as a real study. Retrospective (reanalysis) forcings are the right tool for evaluating model performance against history, because the period is fixed and the forcings are a consistent, quality-controlled dataset. This run covers a full week.

```bash
./scripts/datastream \
  -s 201906100100 \
  -e 201906170000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_2019_retro \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json \
  -D PROVO_RIVER_WOODLAND \
  -n 8
```

What's worth understanding:

- **`-C NWM_RETRO_V3`** — the retrospective forcing source. Choose `NWM_RETRO_V3` for the v3 reanalysis or `NWM_RETRO_V2` for v2. Unlike the operational sources in [Example 8](#example-8--operational-forecasts-and-daily-mode), retrospective sources take your `-s`/`-e` window **literally** — there's no forecast cycle or ensemble suffix to specify.
- **Time domain.** `-s 201906100100 -e 201906170000` is a 7-day window (168 hourly steps). Memory and runtime scale with **both** the number of catchments and the number of time steps, so a week costs meaningfully more than a day. Size `-n` and your host accordingly (see [USAGE.md](USAGE.md)).
- **`-D PROVO_RIVER_WOODLAND`** — naming the domain (see [Example 3](#example-3--name-the-domain--d)) matters more here than in a throwaway run: the name is recorded in the run's metadata, so a study you come back to months later identifies itself.

Outputs land in `data/provo_2019_retro/ngen-run/outputs/`. For a fully manual, step-by-step version of a retrospective study (calling forcingprocessor, config generation, and NGIAB yourself), see [BREAKDOWN.md](BREAKDOWN.md).

---

## Example 8 — Operational forecasts and `DAILY` mode

To run against **operational** NWM products (short-range, medium-range, analysis-and-assimilation) rather than the retrospective archive, you use a structured `FORCING_SOURCE` string and, usually, `DAILY` mode. This example runs today's 06Z short-range forecast.

```bash
./scripts/datastream \
  -s DAILY \
  -C NWM_V3_SHORT_RANGE_06 \
  -d $(pwd)/data/provo_short_range \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json \
  -n 4
```

New concepts:

- **`-s DAILY`** with **no `-e`** — DataStreamCLI computes the time window automatically for the current UTC day. If the requested forecast init cycle is later than the current hour, it steps back a day so the data exists. This is how a scheduled daily job "just runs" without anyone editing dates. (You *may* pass `-e YYYYMMDD…` alongside `DAILY` to pin it to a specific day.)

- **`-C NWM_V3_SHORT_RANGE_06`** — the operational forcing-source format. The pattern is:
  ```
  NWM_<VERSION>_<RUN_TYPE>_<INIT_CYCLE>[_<ENSEMBLE_MEMBER>]
  ```
  - `SHORT_RANGE_06` → the short-range forecast, **06Z** init cycle (18 forecast hours). The trailing two digits are the init cycle.
  - `MEDIUM_RANGE_00_1` → the medium-range forecast, **00Z** init cycle, ensemble **member 1** (240 forecast hours). Here the two-digit group is the cycle and the final digit is the ensemble member.
  - `ANALYSIS_ASSIM` and `ANALYSIS_ASSIM_EXTEND` are also available; real-time NOMADS products use `NOMADS` and `NOMADS_POSTPROCESSED`.
  - Availability differs by product — operational short-range files are kept only for roughly the last 30 days, whereas retrospective covers decades. Choose a cycle/day that still exists.

> You may see a line like `FORCING_SOURCE NWM_V3_SHORT_RANGE_06 not among options` early in the output. It's a non-fatal notice (the tool validates the detailed source string later) — the run proceeds normally.

**Writing results to S3 (optional).** Add `-S` and `-o` to upload the run. This needs an S3 bucket you own and `awscli` credentials, so it's shown separately rather than in the runnable command above:
```bash
  # add these flags to write to s3://YOUR_BUCKET/runs/provo/<run-date>/short_range/
  -S YOUR_BUCKET \
  -o runs/provo/DAILY/short_range
```
- **`-S`** (`--S3_BUCKET`) — the destination bucket.
- **`-o`** (`--S3_PREFIX`) — the key prefix within it. If the prefix contains the literal token `DAILY`, it's replaced with the actual run date (e.g. `20240722`), so scheduled jobs write to dated folders automatically.

This command is essentially one VPU's slice of the operational Research DataStream — which brings us to the next example.

---

## Example 9 — Reproduce a NextGen Research DataStream (NRDS) simulation (`-c`)

The [NextGen Research DataStream](https://datastream.ciroh.org) publishes NextGen forcings and outputs for the whole CONUS every day, VPU by VPU, using this exact tool. Because DataStreamCLI versions everything and accepts remote inputs, you can reproduce any published run on your own machine.

### 9a. Reproduce today's operational cycle

This reproduces today's **06Z short-range** simulation for **VPU 09**, pulling the same geopackage and realization the NRDS uses — note that both are remote URLs, so there's nothing to download first:

```bash
./scripts/datastream \
  --START_DATE DAILY \
  --FORCING_SOURCE NWM_V3_SHORT_RANGE_06 \
  --DATA_DIR $(pwd)/data/nrds_vpu09 \
  --REALIZATION https://ciroh-community-ngen-datastream.s3.us-east-1.amazonaws.com/realizations/realization_VPU_09.json \
  --GEOPACKAGE https://datastream-resources.s3.us-east-1.amazonaws.com/VPU_09/config/nextgen_VPU_09.gpkg \
  --NPROCS 8
```

There's nothing new here — it's Examples 1, 3, and 8 combined — and that's the point: **an NRDS run is just a DataStreamCLI command.** (This example uses the long-form flag names, which are interchangeable with the short flags used elsewhere.) VPU 09 is ~11,000 catchments, so give it a capable host and a few minutes.

### 9b. Reproduce a *specific past* NRDS run exactly (`-c`)

To reproduce a run from a particular date, you don't need to reconstruct the command by hand. Every DataStreamCLI run writes a **self-describing configuration file** to `datastream-metadata/datastream.env`, and the NRDS uploads it alongside its outputs. That file records the run options *and* the exact Docker image versions used — so handing it back to DataStreamCLI reproduces the run.

The `-c` / `--CONF_FILE` argument takes such a file. Variables it sets **override** the equivalent CLI args.

```bash
# 1. download the datastream.env for the run you want to reproduce. Path pattern
#    (pick a recent date — see the note below):
#    .../outputs/<model>/v2.2_hydrofabric/ngen.<YYYYMMDD>/<run_type>/<init_cycle>/VPU_<id>/datastream-metadata/datastream.env
curl -sL -o repro.env \
  "https://ciroh-community-ngen-datastream.s3.amazonaws.com/outputs/cfe_nom/v2.2_hydrofabric/ngen.20260722/short_range/00/VPU_09/datastream-metadata/datastream.env"

# 2. reproduce — pass a fresh local DATA_DIR with -d; the Docker image versions are already pinned inside repro.env
./scripts/datastream \
  -c $(pwd)/repro.env \
  -d $(pwd)/data/nrds_repro_20260722
```

- **`-c $(pwd)/repro.env`** — source all run options from this file. Because the file also sets `DS_TAG`, `FP_TAG`, and `NGIAB_TAG`, you get the same container versions the NRDS used at the time, without setting them yourself.
- **`-d $(pwd)/data/nrds_repro_20260722`** — a fresh local output directory for the reproduction (it must not already exist, per the rules in [Before you begin](#before-you-begin)).

> **The date above is just an example.** NRDS keeps operational forcings for only ~30 days, so if the run you pick has been purged (you'll see a 404 fetching `NGEN_FORCINGS` during the run), grab a more recent one — browse published runs at [datastream.ciroh.org](https://datastream.ciroh.org), or list the `outputs/cfe_nom/v2.2_hydrofabric/` prefix for a recent `ngen.<date>`.

DataStreamCLI writes the SHA-256 hash of every container it used to `datastream-metadata/docker_hashes.txt`, plus a `merkdir.file` that lets you prove which files went into the run — so you can verify your reproduction matches the original. (If you'd rather set the container versions by hand, see the [Docker section](#docker-image-versions-and-environment-variables) below.)

---

## Example 10 — Run an LSTM ensemble (`-L`)

Some arguments only matter for specific models. The `-L` argument applies when your realization uses the **LSTM** model: it selects which pretrained ensemble members to run. This example uses the repo's Rust-LSTM realization template.

```bash
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_lstm \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_rust_lstm.json \
  -L 025 \
  -n 4
```

- **`-R …/realization_rust_lstm.json`** — a realization that runs the LSTM model (the pretrained models ship inside the NGIAB container, so there's nothing extra to download).
- **`-L 025`** (`--LSTM_ENS_MEMBERS`) — run ensemble members **0, 2, and 5**. You concatenate the member indices; `-L 0` runs a single member, `-L 012345` runs all six. The index-to-model mapping is documented in [DATASTREAM_OPTIONS.md](DATASTREAM_OPTIONS.md#lstm-enesmble-member-mapping).

The repo also includes `realization_python_lstm_troute.json` and `realization_rust_lstm_troute.json` if you want LSTM coupled with t-route routing.

---

## Example 11 — Warm-start t-route with a restart file (`-t`, `-w`)

This is a model-specific option for the **t-route** channel-routing model: instead of starting routing from a cold state, you can warm-start it from a prior run's state.

```bash
# TEMPLATE — needs a t-route restart file and crosswalk for YOUR domain,
# so it is not part of the copy-paste sequence above.
./scripts/datastream \
  -s 202006200100 \
  -e 202006210000 \
  -C NWM_RETRO_V3 \
  -d $(pwd)/data/provo_restart \
  -g $(pwd)/gage-10154200_subset.gpkg \
  -R $(pwd)/configs/ngen/realization_sloth_nom_cfe_pet_troute.json \
  -t /path/to/troute_restart.nc \
  -w /path/to/crosswalk.nc \
  -n 4
```

- **`-t`** (`--TROUTE_RESTART`) — the t-route restart file holding channel state to resume from. If its path contains the token `DAILY`, it's date-substituted like the S3 prefix, which is convenient for chained daily runs.
- **`-w`** (`--TROUTE_CROSSWALK`) — the crosswalk file that maps the restart's channels onto your hydrofabric. **Required whenever `-t` is used** — a restart without its crosswalk is rejected.

Both files must correspond to the same hydrofabric domain as your geopackage, which is why there's no universal example file — you generate them from a prior routing run over the same domain. Replace the `-t`/`-w` paths with your own to use this.

---

## Docker image versions and environment variables

DataStreamCLI orchestrates several Docker containers. You normally don't manage them — if an image isn't present locally, it's pulled automatically. But a few environment variables give you control, and they're essential for reproducibility.

**Image version tags.** By default these come from [`versions.yml`](../versions.yml) and [`versions_integrations.yml`](../versions_integrations.yml). Override any of them by exporting the variable before running:

| Variable | Container | Example Version |
|----------|-----------|---------------------|
| `DS_TAG` | `awiciroh/datastream` (config generation, validation, conversions) | `1.7.1` |
| `FP_TAG` | `awiciroh/forcingprocessor` (forcings) | `2.2.1` |
| `NGIAB_TAG` | `awiciroh/ciroh-ngen-image` (the NextGen engine, NGIAB) | `v1.8.0` |

```bash
export DS_TAG=1.7.0
export FP_TAG=2.2.0
export NGIAB_TAG=v1.7.0
./scripts/datastream …
```

**Reproducing older NRDS data.** DataStreamCLI is tested to keep the current script working against the container combinations the NRDS has actually deployed. As of this writing those combinations are:

| `DS_TAG` | `FP_TAG` | `NGIAB_TAG` |
|----------|----------|-------------|
| `1.3.0` | `2.0.0` | `v1.6.0` |
| `1.4.0` | `2.0.0` | `v1.6.0` |
| `1.5.0` | `2.0.1` | `v1.7.0` |
| `1.6.0` | `2.1.0` | `v1.7.0` |
| `1.7.0` | `2.2.0` | `v1.7.0` |
| `1.7.1` | `2.2.1` | `v1.8.0` |

When you reproduce a run with its `datastream.env` ([Example 9b](#9b-reproduce-a-specific-past-nrds-run-exactly--c)), these tags come along automatically. To set them by hand, pick the row matching the deployment you're reproducing. (The current script passes newer options — like t-route restart files — to containers via environment variables specifically so older images ignore what they don't understand.)

**Behavior toggles.** Two more environment variables skip steps, handy when you've already validated inputs or supplied your own configs:

| Variable | Effect |
|----------|--------|
| `SKIP_VALIDATION` | Set to `True` to bypass the built-in input-directory validation. |
| `SKIP_BMI_CONFIG_GEN` | Set to `True` to skip BMI config generation (use the configs you supplied). |

---

## Where to go next

- **[The interactive guide](../scripts/datastream_guide)** — build a command by answering prompts.
- **[DATASTREAM_OPTIONS.md](DATASTREAM_OPTIONS.md)** — the canonical reference table for every argument.
- **[USAGE.md](USAGE.md)** — sizing runs to your hardware (memory, processes, runtime).
- **[STANDARD_DIRECTORIES.md](STANDARD_DIRECTORIES.md)** — input/output directory conventions in detail.
- **[NGEN_MODELS.md](NGEN_MODELS.md)** — which NextGen models are supported and auto-configured.
- **[BREAKDOWN.md](BREAKDOWN.md)** — do every step manually to see what DataStreamCLI automates.
- **[CIROH DevCon 2025 workshop](CIROH_devcon_2025/workshop.md)** — a guided tour of the Research DataStream and the tooling.

Found something confusing or broken? Open an issue on the [repository](https://github.com/CIROH-UA/datastreamcli) — the maintainers actively improve the docs and error messages based on feedback.
