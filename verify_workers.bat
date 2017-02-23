@echo off

pushd C:\verify-workers

set GIT=C:\Program Files\Git\bin\git.exe

echo.
echo Resetting local git repository...
"%GIT%" reset --hard

echo.
echo Pulling latest code from branch...
"%GIT%" pull origin prod

echo.
echo Setting correct path...
set PATH=C:\Python27_64bit;C:\Python27_64bit\Scripts;%SystemRoot%\system32

echo.
echo Starting verify workers...
%USERPROFILE%\Envs\verify-workers\Scripts\python.exe -u verify_workers.py %*
