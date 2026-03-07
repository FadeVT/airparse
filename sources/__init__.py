"""Device source backends for pulling capture data from remote devices."""

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import paramiko

log = logging.getLogger(__name__)

CONFIG_DIR = Path(os.environ.get('XDG_CONFIG_HOME', Path.home() / '.config')) / 'airparse'
SOURCES_FILE = CONFIG_DIR / 'sources.json'
MANIFEST_FILE = CONFIG_DIR / 'pull_manifest.json'
PULL_DIR = Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local' / 'share')) / 'airparse' / 'pulls'


@dataclass
class SourceConfig:
    source_type: str
    name: str
    host: str
    port: int = 22
    user: str = 'pi'
    auth: str = 'key'  # 'key' or 'password'
    key_file: str = '~/.ssh/id_ed25519'
    password: str = ''
    remote_path: str = ''
    file_types: list[str] = field(default_factory=list)
    enabled: bool = True

    def to_dict(self) -> dict:
        return {
            'type': self.source_type,
            'name': self.name,
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'auth': self.auth,
            'key_file': self.key_file,
            'password': self.password,
            'remote_path': self.remote_path,
            'file_types': self.file_types,
            'enabled': self.enabled,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'SourceConfig':
        return cls(
            source_type=d.get('type', 'custom'),
            name=d.get('name', 'Unknown'),
            host=d.get('host', ''),
            port=d.get('port', 22),
            user=d.get('user', 'pi'),
            auth=d.get('auth', 'key'),
            key_file=d.get('key_file', '~/.ssh/id_ed25519'),
            password=d.get('password', ''),
            remote_path=d.get('remote_path', ''),
            file_types=d.get('file_types', []),
            enabled=d.get('enabled', True),
        )


@dataclass
class RemoteFile:
    """A file available on a remote device."""
    path: str
    size: int
    mtime: int
    source_name: str


@dataclass
class PullResult:
    """Result of pulling files from a source."""
    source_name: str
    files_pulled: list[str] = field(default_factory=list)
    files_skipped: int = 0
    errors: list[str] = field(default_factory=list)
    total_bytes: int = 0


DEFAULT_SOURCES = [
    SourceConfig(
        source_type='kismet',
        name='Kismet RPi5',
        host='192.168.1.204',
        user='fade',
        remote_path='/home/fade/Documents/Kismet/',
        file_types=['.kismet'],
        enabled=True,
    ),
    SourceConfig(
        source_type='pwnagotchi',
        name='Pwnagotchi',
        host='10.0.0.2',
        user='pi',
        remote_path='/home/pi/handshakes/',
        file_types=['.pcap'],
        enabled=True,
    ),
    SourceConfig(
        source_type='pager',
        name='Hak5 Pager',
        host='172.16.52.1',
        user='root',
        remote_path='/mmc/root/loot/handshakes/',
        file_types=['.pcap', '.22000'],
        enabled=True,
    ),
]


def load_sources() -> list[SourceConfig]:
    """Load source configs from disk, creating defaults if needed."""
    if SOURCES_FILE.exists():
        try:
            data = json.loads(SOURCES_FILE.read_text())
            return [SourceConfig.from_dict(s) for s in data.get('sources', [])]
        except (json.JSONDecodeError, KeyError) as e:
            log.warning("Failed to load sources config: %s", e)
    return list(DEFAULT_SOURCES)


def save_sources(sources: list[SourceConfig]):
    """Save source configs to disk."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    data = {'sources': [s.to_dict() for s in sources]}
    SOURCES_FILE.write_text(json.dumps(data, indent=2))


def load_manifest() -> dict:
    """Load pull manifest tracking previously pulled files."""
    if MANIFEST_FILE.exists():
        try:
            return json.loads(MANIFEST_FILE.read_text())
        except (json.JSONDecodeError, KeyError):
            pass
    return {}


def save_manifest(manifest: dict):
    """Save pull manifest."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2))


class DeviceSource:
    """Base class for device source backends."""

    def __init__(self, config: SourceConfig):
        self.config = config
        self._ssh: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def source_type(self) -> str:
        return self.config.source_type

    def _connect_ssh(self) -> paramiko.SSHClient:
        """Establish SSH connection."""
        if self._ssh is not None:
            try:
                self._ssh.exec_command('true', timeout=3)
                return self._ssh
            except Exception:
                self._close()

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        kwargs = {
            'hostname': self.config.host,
            'port': self.config.port,
            'username': self.config.user,
            'timeout': 5,
            'banner_timeout': 5,
            'auth_timeout': 5,
        }

        if self.config.auth == 'password' and self.config.password:
            kwargs['password'] = self.config.password
            kwargs['look_for_keys'] = False
        else:
            key_path = Path(self.config.key_file).expanduser()
            if key_path.exists():
                kwargs['key_filename'] = str(key_path)
            # Fall back to SSH agent / default keys

        ssh.connect(**kwargs)
        self._ssh = ssh
        return ssh

    def _get_sftp(self) -> paramiko.SFTPClient:
        """Get or create SFTP session."""
        if self._sftp is not None:
            try:
                self._sftp.listdir('.')
                return self._sftp
            except Exception:
                pass
        ssh = self._connect_ssh()
        self._sftp = ssh.open_sftp()
        return self._sftp

    def _close(self):
        if self._sftp:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None
        if self._ssh:
            try:
                self._ssh.close()
            except Exception:
                pass
            self._ssh = None

    def probe(self) -> bool:
        """Check if the device is reachable."""
        if not self.config.host:
            return False
        try:
            self._connect_ssh()
            return True
        except Exception:
            return False
        finally:
            self._close()

    def list_files(self) -> list[RemoteFile]:
        """List available capture files on the device."""
        try:
            sftp = self._get_sftp()
            remote_path = self.config.remote_path
            files = []

            for entry in sftp.listdir_attr(remote_path):
                if not entry.filename.startswith('.'):
                    ext = Path(entry.filename).suffix.lower()
                    if ext in self.config.file_types or not self.config.file_types:
                        files.append(RemoteFile(
                            path=f"{remote_path.rstrip('/')}/{entry.filename}",
                            size=entry.st_size or 0,
                            mtime=entry.st_mtime or 0,
                            source_name=self.config.name,
                        ))

            return sorted(files, key=lambda f: f.mtime, reverse=True)
        except Exception as e:
            log.warning("Failed to list files on %s: %s", self.config.name, e)
            return []

    def pull_files(
        self,
        dest_dir: Path,
        manifest: dict,
        only_new: bool = True,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
    ) -> PullResult:
        """Pull capture files from the device.

        Args:
            dest_dir: Local directory to store pulled files.
            manifest: Pull manifest dict for tracking.
            only_new: If True, skip files already in manifest with same mtime/size.
            progress_cb: Callback(filename, bytes_transferred, total_bytes).

        Returns:
            PullResult with pulled files and stats.
        """
        result = PullResult(source_name=self.config.name)
        dest_dir.mkdir(parents=True, exist_ok=True)

        try:
            sftp = self._get_sftp()
            files = self.list_files()

            for rf in files:
                manifest_key = f"{self.config.name}:{rf.path}"

                if only_new and manifest_key in manifest:
                    prev = manifest[manifest_key]
                    if prev.get('mtime') == rf.mtime and prev.get('size') == rf.size:
                        result.files_skipped += 1
                        continue

                local_name = Path(rf.path).name
                local_path = dest_dir / local_name

                # Handle name collisions
                counter = 1
                while local_path.exists():
                    stem = Path(rf.path).stem
                    suffix = Path(rf.path).suffix
                    local_path = dest_dir / f"{stem}_{counter}{suffix}"
                    counter += 1

                try:
                    def _progress(transferred, total, _name=local_name):
                        if progress_cb:
                            progress_cb(_name, transferred, total)

                    sftp.get(rf.path, str(local_path), callback=_progress)
                    result.files_pulled.append(str(local_path))
                    result.total_bytes += rf.size

                    manifest[manifest_key] = {
                        'mtime': rf.mtime,
                        'size': rf.size,
                        'local_path': str(local_path),
                        'pulled_at': __import__('datetime').datetime.now().isoformat(),
                    }
                except Exception as e:
                    result.errors.append(f"{rf.path}: {e}")

        except Exception as e:
            result.errors.append(f"Connection failed: {e}")
        finally:
            self._close()

        return result

    def scan_additional_paths(self) -> list[str]:
        """Scan for additional files (WiGLE CSVs, etc.) beyond the main capture path.

        Override in subclasses for source-specific discovery.
        """
        return []
