' Launch Big Data Tool - VBScript launcher
' Run the Python GUI application with pythonw (no console window)

Set WshShell = CreateObject("WScript.Shell")

' Get the directory where this script is located
strScriptPath = WScript.ScriptFullName
Set objFSO = CreateObject("Scripting.FileSystemObject")
strScriptDir = objFSO.GetParentFolderName(strScriptPath)

' Change to that directory and run the Python GUI
WshShell.CurrentDirectory = strScriptDir
WshShell.Run "pythonw.exe AppCore\gui_app.py", 0, False
