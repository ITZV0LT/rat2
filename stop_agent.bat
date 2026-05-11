@echo off
:: Stops all running RAT2 agent processes (visible or hidden)
wmic process where "name='python.exe' and commandline like '%%agent.py%%'" delete >nul 2>&1
wmic process where "name='pythonw.exe' and commandline like '%%agent.py%%'" delete >nul 2>&1
echo RAT2 Agent stopped.
