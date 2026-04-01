Set WshShell = CreateObject("WScript.Shell")

pythonPath = "C:\Users\mindaugas.bukauskas\AppData\Local\Microsoft\WindowsApps\python.exe"
scriptPath = "C:\Users\mindaugas.bukauskas\OneDrive - CSG\Documents\NS\synopticom_nps_watch_MB_FIXED_state_temp.py"

logPath = WshShell.ExpandEnvironmentStrings("%TEMP%") & "\synopticom_watch.log"

cmd = """" & pythonPath & """ """ & scriptPath & """ >> """ & logPath & """ 2>&1"

WshShell.Run "cmd.exe /c " & cmd, 0, False