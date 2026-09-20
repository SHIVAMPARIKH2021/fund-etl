## Fund ETL Job
- This job loads Mutual Fund data into Postgresql database.

### Example Run Command

- Step-1
```Bash
find . -type d -name "__pycache__" -exec rm -rf {} +
```

- Step-2:
```Bash
uv run python main.py --year 2026 --quarter 2 --ticker False
```
- If ```--ticker```  is set to ```True```   than company ticker data will be imported else it will be skipped.