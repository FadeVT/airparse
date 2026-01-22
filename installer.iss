[Setup]
AppName=Kismet GUI Reader
AppVersion=1.0.0
AppPublisher=FadeVT
AppPublisherURL=https://github.com/FadeVT/kismet-gui-reader
AppSupportURL=https://github.com/FadeVT/kismet-gui-reader/issues
AppUpdatesURL=https://github.com/FadeVT/kismet-gui-reader/releases
DefaultDirName={autopf}\Kismet GUI Reader
DefaultGroupName=Kismet GUI Reader
AllowNoIcons=yes
LicenseFile=
OutputDir=installer_output
OutputBaseFilename=KismetGUIReader_Setup_1.0.0
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
Source: "dist\KismetGUIReader.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Kismet GUI Reader"; Filename: "{app}\KismetGUIReader.exe"
Name: "{group}\{cm:UninstallProgram,Kismet GUI Reader}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\Kismet GUI Reader"; Filename: "{app}\KismetGUIReader.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\KismetGUIReader.exe"; Description: "{cm:LaunchProgram,Kismet GUI Reader}"; Flags: nowait postinstall skipifsilent
