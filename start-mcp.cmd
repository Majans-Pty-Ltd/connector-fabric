@echo off
REM Acquires a Fabric user token via MSAL and launches mcp-remote as a
REM stdio-to-StreamableHTTP proxy. Add to Claude Code MCP config as:
REM
REM   "fabric": {
REM     "type": "stdio",
REM     "command": "cmd",
REM     "args": ["/c", "C:\\...\\connector-fabric\\start-mcp.cmd"]
REM   }
REM
REM First run: opens browser for device-code login.
REM Subsequent runs: silently refreshes cached token.

set SCRIPT_DIR=%~dp0

REM Get fresh token (--force-refresh ensures full ~60 min lifetime, not a stale
REM cached token that's about to expire mid-session)
for /f "usebackq delims=" %%t in (`python "%SCRIPT_DIR%get-user-token.py" --force-refresh`) do set FABRIC_TOKEN=%%t

if "%FABRIC_TOKEN%"=="" (
    echo ERROR: Failed to acquire Fabric token. >&2
    echo Run 'python "%SCRIPT_DIR%get-user-token.py"' manually to authenticate. >&2
    exit /b 1
)

REM Launch mcp-remote as stdio proxy to the remote StreamableHTTP MCP server
npx -y mcp-remote@latest https://fabric.majans.com/mcp/ --header "Authorization: Bearer %FABRIC_TOKEN%"
