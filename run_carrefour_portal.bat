@echo off
REM Run the Carrefour Order Portal (Streamlit)
REM Adjust the path below if you place the file elsewhere.

python -m streamlit run "%~dp0carrefour_order_portal.py"
pause
