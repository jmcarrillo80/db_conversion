@echo off
@echo.
@echo Converting tables ...
start "Converting tables" /min /wait "C:\Users\Jorge.Carrillo1\Documents\DESARROLLO\VS CODE\DB_CONVERSION_REPO\db_conversion\venv\Scripts\python" "C:\Users\Jorge.Carrillo1\Documents\DESARROLLO\VS CODE\DB_CONVERSION_REPO\db_conversion\app.py" %1 %2 %3
@echo.
@echo Tables converted