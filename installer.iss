[Setup]
AppName=AirParse
AppVersion=1.0.0
AppPublisher=FadeVT
AppPublisherURL=https://github.com/FadeVT/airparse
AppSupportURL=https://github.com/FadeVT/airparse/issues
AppUpdatesURL=https://github.com/FadeVT/airparse/releases
DefaultDirName={autopf}\AirParse
DefaultGroupName=AirParse
AllowNoIcons=yes
LicenseFile=
OutputDir=installer_output
OutputBaseFilename=AirParse_Setup_1.0.0
Compression=lzma
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "dist\AirParse.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\AirParse"; Filename: "{app}\AirParse.exe"
Name: "{group}\{cm:UninstallProgram,AirParse}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\AirParse"; Filename: "{app}\AirParse.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\AirParse.exe"; Description: "{cm:LaunchProgram,AirParse}"; Flags: nowait postinstall skipifsilent
