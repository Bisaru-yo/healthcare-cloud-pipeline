@echo off
setlocal
pushd "%~dp0.."
if errorlevel 1 exit /b 1
docker compose --env-file .env --env-file local/airflow.env -f compose.yaml -f compose.airflow.yaml %*
set "TASK_STATUS=%ERRORLEVEL%"
popd
exit /b %TASK_STATUS%
