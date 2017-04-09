@echo off

rem Lock screen immediately
rundll32.exe user32.dll, LockWorkStation

rem Call batch file
call C:\verify-workers\verify_workers.bat start workers
