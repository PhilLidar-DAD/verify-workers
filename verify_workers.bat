@echo off

pushd C:\verify-workers

set GIT=C:\Program Files\Git\bin\git.exe

echo.
echo Resetting local git repository...
"%GIT%" reset --hard

echo.
echo Pulling latest code from branch...
"%GIT%" pull origin

echo.
echo Starting verify workers...
C:\Users\admin\Envs\verify-workers\Scripts\python.exe -u verify_workers.py %*
