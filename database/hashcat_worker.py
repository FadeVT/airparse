"""Background worker for hashcat WPA password cracking pipeline."""

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from PyQt6.QtCore import QThread, pyqtSignal


class HashcatWorker(QThread):
    """Runs hcxpcapngtool + hashcat pipeline in a background thread."""

    status = pyqtSignal(str)                # Human-readable status
    progress = pyqtSignal(dict)             # Parsed hashcat --status-json
    finished = pyqtSignal(bool, str, str)   # success, password_or_error, bssid_key

    def __init__(self, pcap_path: str, bssid: str, client_mac: str,
                 ssid: str, wordlist_path: str, use_gpu: bool = True):
        super().__init__()
        self.pcap_path = pcap_path
        self.bssid = bssid
        self.client_mac = client_mac
        self.ssid = ssid
        self.wordlist_path = wordlist_path
        self.use_gpu = use_gpu
        self.bssid_key = f"{bssid}:{client_mac}"

        self._cancelled = False
        self._process: subprocess.Popen | None = None
        self._temp_dir: str | None = None

    def run(self):
        try:
            self._temp_dir = tempfile.mkdtemp(prefix='kismet_crack_')

            # Step 1: Convert pcap to hc22000
            self.status.emit("Converting capture to hashcat format...")
            hc22000_path = self._run_hcxpcapngtool()
            if self._cancelled:
                return

            if not hc22000_path:
                self.finished.emit(False, "hcxpcapngtool produced no output", self.bssid_key)
                return

            # Step 2: Filter for target BSSID
            self.status.emit("Extracting target handshake...")
            filtered_path = self._filter_hc22000(hc22000_path)
            if self._cancelled:
                return

            if not filtered_path:
                self.finished.emit(
                    False,
                    f"No extractable handshake found for {self.ssid} ({self.bssid})",
                    self.bssid_key)
                return

            # Step 3: Run hashcat
            self.status.emit(f"Cracking {self.ssid}...")
            found, password = self._run_hashcat(filtered_path)
            if self._cancelled:
                return

            if found:
                self.finished.emit(True, password, self.bssid_key)
            else:
                self.finished.emit(False, "Password not found in wordlist", self.bssid_key)

        except Exception as e:
            self.finished.emit(False, str(e), self.bssid_key)
        finally:
            self._cleanup()

    def _run_hcxpcapngtool(self) -> str | None:
        """Convert pcap to hc22000 format. Returns path to output file."""
        output_path = os.path.join(self._temp_dir, 'full.hc22000')

        try:
            result = subprocess.run(
                ['hcxpcapngtool', '-o', output_path, self.pcap_path],
                capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            return None

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return output_path
        return None

    def _filter_hc22000(self, hc22000_path: str) -> str | None:
        """Filter hc22000 for target BSSID. Returns path to filtered file."""
        target_hex = self.bssid.replace(':', '').lower()
        filtered_path = os.path.join(self._temp_dir, 'target.hc22000')

        with open(hc22000_path, 'r') as f:
            lines = f.readlines()

        # hc22000 format: WPA*TYPE*MIC/PMKID*MAC_AP*MAC_STA*ESSID*...
        # Field 3 (0-indexed) is MAC_AP as 12 hex chars
        matched = []
        for line in lines:
            parts = line.strip().split('*')
            if len(parts) >= 4 and parts[3].lower() == target_hex:
                matched.append(line)

        if not matched:
            return None

        with open(filtered_path, 'w') as f:
            f.writelines(matched)

        return filtered_path

    def _run_hashcat(self, filtered_path: str) -> tuple[bool, str]:
        """Run hashcat and monitor progress. Returns (found, password)."""
        outfile = os.path.join(self._temp_dir, 'cracked.txt')

        cmd = [
            'hashcat',
            '-m', '22000',
            '-a', '0',
            '--status', '--status-json', '--status-timer=2',
            f'--outfile={outfile}',
            '--potfile-disable',
            filtered_path,
            self.wordlist_path,
        ]

        if not self.use_gpu:
            cmd.insert(1, '-D')
            cmd.insert(2, '1')

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1)

            # Read stdout line by line for JSON status updates
            for line in self._process.stdout:
                if self._cancelled:
                    self._process.terminate()
                    return False, ''

                line = line.strip()
                if not line:
                    continue

                parsed = self._parse_status_line(line)
                if parsed:
                    self.progress.emit(parsed)

            self._process.wait()

        except Exception as e:
            if self._process:
                self._process.terminate()
            return False, str(e)
        finally:
            self._process = None

        # Check outfile for cracked password
        if os.path.exists(outfile) and os.path.getsize(outfile) > 0:
            with open(outfile, 'r') as f:
                for line in f:
                    # Format: hash:password
                    parts = line.strip().rsplit(':', 1)
                    if len(parts) == 2 and parts[1]:
                        return True, parts[1]

        return False, ''

    def _parse_status_line(self, line: str) -> dict | None:
        """Parse a hashcat --status-json line."""
        if not line.startswith('{'):
            return None
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None

    def cancel(self):
        """Cancel the running operation."""
        self._cancelled = True
        if self._process:
            try:
                self._process.terminate()
            except OSError:
                pass

    def _cleanup(self):
        """Remove temp directory."""
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except OSError:
                pass
