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
echo Setting correct environment variables...
set PATH=C:\Python27;C:\Python27\Scripts;%SystemRoot%\system32
set GDAL_DATA=
set GDAL_DRIVER_PATH=

echo.
echo Reinstalling requirements...
%USERPROFILE%\Envs\verify-workers\Scripts\pip.exe install -r requirements.txt

echo.
echo Starting verify workers...
%USERPROFILE%\Envs\verify-workers\Scripts\python.exe -u verify_workers.py %*
