@echo off
cd /d "%~dp0"
title RAT2 Agent
set RAT2_URL=ws://localhost:8080/ws/agent
set RAT2_LOCATION=This PC
set RAT2_KEY=Zu6_4hEGklhBBQzHYjj1-0n2hbvr-6cuu4huzkufhZQ
python -u rat2.py agent
pause
