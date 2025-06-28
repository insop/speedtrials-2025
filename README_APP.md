# Georgia Water Quality Explorer

## 1. Install & ingest

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python ingest_data.py        # < 1 min, creates georgia_water.db