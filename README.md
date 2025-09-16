# dagster_tutorial

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then active the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

## Tree

```bash
.
├── data
├── src
│   └── pic_automation_dev
│       ├── defs
│       │   ├── assets
│       │   │   ├── bot_cust_carr_xwalk.py
│       │   │   ├── cstm.py
│       │   │   ├── cust.py
│       │   │   ├── fld.py
│       │   │   ├── fmac_dtl.py
│       │   │   ├── fmac_pic.py
│       │   │   ├── fmac_xwalk.py
│       │   │   ├── inpt_file.py
│       │   │   ├── inpt.py
│       │   │   ├── pic_fl_upld_data.py
│       │   │   ├── pic_fl_upld_log.py
│       │   │   ├── pic_trx_log.py
│       │   │   ├── pic.py
│       │   │   ├── prfl_dtl.py
│       │   │   ├── prfl.py
│       │   │   ├── rpt_rvw_line.py
│       │   │   ├── rpt_rvw.py
│       │   │   ├── rpt.py
│       │   │   ├── snpsht_rvw_line.py
│       │   │   ├── snpsht_rvw.py
│       │   │   ├── snpsht.py
│       │   │   └── upld.py
│       │   ├── constants.py
│       │   ├── jobs.py
│       │   ├── ops.py
│       │   ├── partitions.py
│       │   ├── resources.py
│       │   ├── schedules.py
│       │   └── sensors.py
│       └── definitions.py
└── pyproject.toml
```