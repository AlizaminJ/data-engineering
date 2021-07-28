@ECHO OFF
ECHO ============================
ECHO Replicating data
cd <SCRIPT-DIRECTORY>
"<INCLUDE-YOUR-PATH-TO-PYTHON\venv\Scripts\python.exe>" "INCLUDE-YOUR-PATH-TO-PY-FILE.py"
ECHO Syncing with AWS S3 bucket
aws s3 sync <PATH-TO-FOLDER-CONTAINING-REPLICATED-FILES> <s3://YOUR-BUCKET> --delete --profile <YOUR-AWS-CLI-PROFILE>
EXIT
