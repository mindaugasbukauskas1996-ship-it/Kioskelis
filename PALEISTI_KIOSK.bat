@echo off
cd /d "%~dp0"
call venv\Scripts\activate.bat
python synopticom_nps_watch_MB_codegen_based_NO_FSTRING_HTML_v10_READY.py
pause
