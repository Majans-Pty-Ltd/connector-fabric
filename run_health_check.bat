@echo off
REM Majans MCP Health Check — Daily scheduled task
REM Injects secrets from 1Password, runs health_check.py
REM Scheduled via: schtasks /create /tn "Majans MCP Health" /tr "...\run_health_check.bat" /sc daily /st 07:00

cd /d "%~dp0"
"C:\Users\Amit\AppData\Local\Microsoft\WinGet\Packages\AgileBits.1Password.CLI_Microsoft.Winget.Source_8wekyb3d8bbwe\op.exe" run --env-file=.env.template -- python health_check.py
