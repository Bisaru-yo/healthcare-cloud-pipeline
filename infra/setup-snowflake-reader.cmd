@echo off
setlocal
pushd "%~dp0.."
if errorlevel 1 exit /b 1

set "TASK_ACCOUNT_ID="
for /f "delims=" %%A in ('aws sts get-caller-identity --profile healthcare-cloud --query Account --output text --no-cli-pager') do set "TASK_ACCOUNT_ID=%%A"
if not "%TASK_ACCOUNT_ID%"=="009285528415" goto wrongaccount

echo Creating the dedicated Snowflake S3 reader role...
aws iam create-role --role-name healthcare-snowflake-reader --assume-role-policy-document file://infra/snowflake-reader-trust-initial.json --description "Read-only access to healthcare batch files for a Snowflake storage integration" --max-session-duration 3600 --tags Key=Project,Value=HealthcarePipeline --profile healthcare-cloud --query Role.Arn --output text --no-cli-pager
if errorlevel 1 goto failed

echo Attaching the read-only project S3 policy...
aws iam put-role-policy --role-name healthcare-snowflake-reader --policy-name HealthcareS3ReadOnly --policy-document file://infra/snowflake-reader-policy.json --profile healthcare-cloud --no-cli-pager
if errorlevel 1 goto failed

echo SUCCESS: Snowflake reader role created. No data has been modified.
echo Next run sql\01_snowflake_setup.sql in Snowflake and retain its integration description.
popd
endlocal
exit /b 0

:wrongaccount
echo Setup stopped: healthcare-cloud must authenticate to AWS account 009285528415.
echo Verify the profile or renew its browser login before continuing.
goto cleanupfailure

:failed
echo Setup stopped. Keep the exact error output for troubleshooting.
echo If the role already exists, inspect it before retrying individual commands.

:cleanupfailure
popd
endlocal
exit /b 1
