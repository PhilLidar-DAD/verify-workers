# verify-workers

## Git installation

1. Download latest Git for Windows (if not installed)

https://github.com/git-for-windows/git/releases/download/v2.11.1.windows.1/Git-2.11.1-64-bit.exe

2. Clone repository (Open Git Bash here at C:\)

```
git config --global http.proxy http://datamanager:<password>@proxy.dream.upd.edu.ph:8080
git clone https://github.com/PhilLidar-DAD/verify-workers.git
```

## Virtualenv installation

### Windows

1. Install latest Python 2.7 64-bit (if not installed) to C:\Python27_64bit\

https://www.python.org/ftp/python/2.7.13/python-2.7.13.amd64.msi

2. Create %APPDATA%\pip\pip.ini file (resolves to AppData\Roaming\pip\pip.ini) and add the ff. lines

[global]
proxy = http://datamanager:<password>@proxy.dream.upd.edu.ph:8080

3. Install virtualenvwrapper for Windows (using Command Prompt)

```
> set PATH=C:\Python27_64bit;C:\Python27_64bit\Scripts;%SystemRoot%\system32
> pip.exe install virtualenvwrapper-win
```

4. Create virtualenv

```
> mkvirtualenv.bat verify-workers
```

5. Check python version
```
(verify-workers) > python
Python 2.7.13 (v2.7.13:a06454b1afa1, Dec 17 2016, 20:53:40) [MSC v.1500 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license" for more information.
>>>
```

6. Install requirements.txt

```
(verify-workers) > cd \verify-workers
(verify-workers) > pip install -r requirements.txt
```

7. Create settings.py file

## Disable Windows Error Reporting UI

1. Run regedit

2. Set HKEY_CURRENT_USER\SOFTWARE\Microsoft\Windows\Windows Error Reporting\DontShowUI registry value to 1


alter database verify_workers default character set latin1 default collate latin1_swedish_ci;