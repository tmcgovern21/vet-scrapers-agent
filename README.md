# Vet Scraper System

Agentic scraper + auditor + diagnostic + patcher system for
veterinary directory data collection.

See SPEC.md for architecture. Each folder has its own SPEC.md.

## Quick start
1. All Python execution: `.venv\Scripts\python.exe <script>`
2. Run a scraper: `.venv\Scripts\python.exe scrapers\aaep_scraper.py`
3. Audit the output: `.venv\Scripts\python.exe auditor\run.py --input outputs\aaep_YYYYMMDD.xlsx`
4. (If issues) Diagnose: `.venv\Scripts\python.exe diagnostic\run.py --audit audit_reports\...`
5. (If actionable) Patch: `.venv\Scripts\python.exe patcher\run.py --diagnostic ...`