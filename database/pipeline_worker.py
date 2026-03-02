"""Multi-stage hashcat cracking pipeline worker."""

import json
import os
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path

from PyQt6.QtCore import QThread, pyqtSignal

from config import DEFAULT_CONFIG

# Crack levels map to the maximum stage number to run
CRACK_LEVELS = {
    'quick': 1,
    'standard': 2,
    'deep': 5,
    'exhaustive': 6,
}

# Pipeline stage definitions
STAGES = [
    {
        'num': 1,
        'name': 'Quick Dictionary',
        'type': 'dictionary',
        'rule': None,
    },
    {
        'num': 2,
        'name': 'Light Rules',
        'type': 'dictionary',
        'rule': 'best66.rule',  # Arch ships best66; Debian/Ubuntu ship best64
        'rule_fallback': 'best64.rule',
    },
    {
        'num': 3,
        'name': 'Heavy Rules',
        'type': 'dictionary',
        'rule': 'OneRuleToRuleThemAll.rule',
        'downloadable': True,
    },
    {
        'num': 4,
        'name': 'Common Masks',
        'type': 'mask',
        'masks': [
            '?d?d?d?d?d?d?d?d',
            '?d?d?d?d?d?d?d?d?d',
            '?d?d?d?d?d?d?d?d?d?d',
            '?l?l?l?l?l?l?l?l',
        ],
    },
    {
        'num': 5,
        'name': 'Hybrid Attacks',
        'type': 'hybrid',
        'append_masks': ['?d?d', '?d?d?d?d'],     # -a 6: word + mask
        'prepend_masks': ['?d?d?d?d'],              # -a 7: mask + word
    },
    {
        'num': 6,
        'name': 'Deep Rules',
        'type': 'dictionary',
        'rule': 'dive.rule',
    },
]


class PipelineWorker(QThread):
    """Runs a multi-stage hashcat cracking pipeline in a background thread."""

    stage_changed = pyqtSignal(int, str)        # stage_num, stage_name
    stage_progress = pyqtSignal(dict)           # hashcat --status-json parsed
    hash_cracked = pyqtSignal(str, str, str)    # bssid, client_mac, password
    rule_download_needed = pyqtSignal()         # request UI to download OTRTRA
    finished = pyqtSignal(int, int)             # total_cracked, total_attempted
    error = pyqtSignal(str)

    def __init__(self, pcap_path: str, targets: list[dict],
                 wordlist_paths: list[str], crack_level: str,
                 use_gpu: bool = True):
        super().__init__()
        self.pcap_path = pcap_path
        self.targets = targets          # [{"bssid": ..., "client_mac": ..., "ssid": ...}]
        self.wordlist_paths = wordlist_paths
        self.crack_level = crack_level
        self.use_gpu = use_gpu

        self._cancelled = False
        self._process: subprocess.Popen | None = None
        self._temp_dir: str | None = None

        # Track cracked hashes: bssid_hex -> (client_mac, password)
        self._cracked: dict[str, tuple[str, str]] = {}
        # Map bssid_hex -> list of target dicts (for multi-client same AP)
        self._bssid_targets: dict[str, list[dict]] = {}
        for t in targets:
            bssid_hex = t['bssid'].replace(':', '').lower()
            self._bssid_targets.setdefault(bssid_hex, []).append(t)

        # Event for rule download synchronization
        self._rule_download_event = threading.Event()
        self._rule_download_path: str | None = None

    def run(self):
        try:
            self._temp_dir = tempfile.mkdtemp(prefix='airparse_pipeline_')
            max_stage = CRACK_LEVELS.get(self.crack_level, 2)

            # Step 1: Convert pcap to hc22000
            self.stage_changed.emit(0, 'Converting capture...')
            hc22000_path = self._run_hcxpcapngtool()
            if self._cancelled:
                return

            if not hc22000_path:
                self.error.emit('hcxpcapngtool produced no output — '
                                'no EAPOL/PMKID data found in capture')
                self.finished.emit(0, len(self.targets))
                return

            # Step 2: Filter for target BSSIDs (batch)
            all_lines = self._filter_for_targets(hc22000_path)
            if self._cancelled:
                return

            if not all_lines:
                self.error.emit('No extractable handshakes found for selected targets')
                self.finished.emit(0, len(self.targets))
                return

            # Step 3: Run pipeline stages
            potfile_path = os.path.join(self._temp_dir, 'pipeline.potfile')

            for stage in STAGES:
                if self._cancelled:
                    break
                if stage['num'] > max_stage:
                    break

                # Build working file with only uncracked hashes
                working_path = self._build_working_file(all_lines)
                if not working_path:
                    break  # All cracked

                self.stage_changed.emit(stage['num'], stage['name'])

                if stage['type'] == 'dictionary':
                    self._run_dictionary_stage(stage, working_path, potfile_path)
                elif stage['type'] == 'mask':
                    self._run_mask_stage(stage, working_path, potfile_path)
                elif stage['type'] == 'hybrid':
                    self._run_hybrid_stage(stage, working_path, potfile_path)

                if self._cancelled:
                    break

                # Check potfile for new cracks after stage
                self._check_potfile(potfile_path)

            self.finished.emit(len(self._cracked), len(self.targets))

        except Exception as e:
            self.error.emit(str(e))
            self.finished.emit(len(self._cracked), len(self.targets))
        finally:
            self._cleanup()

    # ── hcxpcapngtool ──────────────────────────────────────────────

    def _run_hcxpcapngtool(self) -> str | None:
        output_path = os.path.join(self._temp_dir, 'full.hc22000')
        try:
            subprocess.run(
                ['hcxpcapngtool', '-o', output_path, self.pcap_path],
                capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            return None

        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return output_path
        return None

    def _filter_for_targets(self, hc22000_path: str) -> list[str]:
        """Filter hc22000 for all target BSSIDs. Returns matching lines."""
        target_hexes = set()
        for t in self.targets:
            target_hexes.add(t['bssid'].replace(':', '').lower())

        with open(hc22000_path, 'r') as f:
            lines = f.readlines()

        matched = []
        for line in lines:
            parts = line.strip().split('*')
            if len(parts) >= 4 and parts[3].lower() in target_hexes:
                matched.append(line)

        return matched

    def _build_working_file(self, all_lines: list[str]) -> str | None:
        """Write uncracked hash lines to a working file. Returns None if all cracked."""
        uncracked = []
        for line in all_lines:
            parts = line.strip().split('*')
            if len(parts) >= 4:
                bssid_hex = parts[3].lower()
                if bssid_hex not in self._cracked:
                    uncracked.append(line)

        if not uncracked:
            return None

        working_path = os.path.join(self._temp_dir, 'working.hc22000')
        with open(working_path, 'w') as f:
            f.writelines(uncracked)
        return working_path

    # ── Stage runners ──────────────────────────────────────────────

    def _run_dictionary_stage(self, stage: dict, hash_path: str, potfile: str):
        """Run dictionary attack (with optional rules) for each wordlist."""
        rule_path = None
        if stage.get('rule'):
            rule_path = self._find_rule(stage['rule'])
            # Try fallback rule name
            if not rule_path and stage.get('rule_fallback'):
                rule_path = self._find_rule(stage['rule_fallback'])
            # Downloadable rule (Stage 3)
            if not rule_path and stage.get('downloadable'):
                rule_path = self._request_rule_download(stage['rule'])
                if not rule_path:
                    return  # Download failed or cancelled

        for wordlist in self.wordlist_paths:
            if self._cancelled:
                return

            cmd = self._base_hashcat_cmd(hash_path, potfile)
            cmd.extend(['-a', '0'])
            if rule_path:
                cmd.extend(['-r', rule_path])
            cmd.append(wordlist)

            self._run_hashcat(cmd, potfile)

    def _run_mask_stage(self, stage: dict, hash_path: str, potfile: str):
        """Run mask/brute-force attacks."""
        for mask in stage.get('masks', []):
            if self._cancelled:
                return

            cmd = self._base_hashcat_cmd(hash_path, potfile)
            cmd.extend(['-a', '3', mask])

            self._run_hashcat(cmd, potfile)

    def _run_hybrid_stage(self, stage: dict, hash_path: str, potfile: str):
        """Run hybrid attacks: dictionary+mask (-a 6) and mask+dictionary (-a 7)."""
        # -a 6: word + appended mask
        for mask in stage.get('append_masks', []):
            for wordlist in self.wordlist_paths:
                if self._cancelled:
                    return
                cmd = self._base_hashcat_cmd(hash_path, potfile)
                cmd.extend(['-a', '6', wordlist, mask])
                self._run_hashcat(cmd, potfile)

        # -a 7: prepended mask + word
        for mask in stage.get('prepend_masks', []):
            for wordlist in self.wordlist_paths:
                if self._cancelled:
                    return
                cmd = self._base_hashcat_cmd(hash_path, potfile)
                cmd.extend(['-a', '7', mask, wordlist])
                self._run_hashcat(cmd, potfile)

    # ── hashcat execution ──────────────────────────────────────────

    def _base_hashcat_cmd(self, hash_path: str, potfile: str) -> list[str]:
        """Build base hashcat command with common flags."""
        cmd = ['hashcat', '-m', '22000', '-O', '-w', '3',
               '--status', '--status-json', '--status-timer=2',
               f'--potfile-path={potfile}',
               hash_path]

        if not self.use_gpu:
            cmd.insert(1, '-D')
            cmd.insert(2, '1')

        return cmd

    def _run_hashcat(self, cmd: list[str], potfile: str):
        """Execute a single hashcat invocation and monitor progress."""
        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1)

            for line in self._process.stdout:
                if self._cancelled:
                    self._process.terminate()
                    return

                line = line.strip()
                if not line:
                    continue

                if line.startswith('{'):
                    try:
                        status = json.loads(line)
                        self.stage_progress.emit(status)
                    except json.JSONDecodeError:
                        pass

            self._process.wait()

        except Exception:
            if self._process:
                try:
                    self._process.terminate()
                except OSError:
                    pass
        finally:
            self._process = None

        # Check for new cracks after this run
        self._check_potfile(potfile)

    # ── Potfile parsing ────────────────────────────────────────────

    def _check_potfile(self, potfile_path: str):
        """Parse potfile for newly cracked hashes and emit signals."""
        if not os.path.exists(potfile_path):
            return

        try:
            with open(potfile_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Format: full_hash_line:password
                    # The hash itself contains * separators, password is after last :
                    colon_idx = line.rfind(':')
                    if colon_idx < 0:
                        continue

                    hash_part = line[:colon_idx]
                    password = line[colon_idx + 1:]
                    if not password:
                        continue

                    # Extract BSSID from hash (field 3, * separated)
                    parts = hash_part.split('*')
                    if len(parts) < 4:
                        continue

                    bssid_hex = parts[3].lower()

                    # Skip already-emitted cracks
                    if bssid_hex in self._cracked:
                        continue

                    # Find matching target(s)
                    target_list = self._bssid_targets.get(bssid_hex, [])
                    if target_list:
                        target = target_list[0]
                        bssid = target['bssid']
                        client_mac = target['client_mac']
                        self._cracked[bssid_hex] = (client_mac, password)
                        self.hash_cracked.emit(bssid, client_mac, password)

        except OSError:
            pass

    # ── Rule file handling ─────────────────────────────────────────

    def _find_rule(self, name: str) -> str | None:
        """Search system and user paths for a hashcat rule file."""
        search_paths = DEFAULT_CONFIG['hashcat'].get('rules_search_paths', [])
        user_dir = DEFAULT_CONFIG['hashcat'].get('user_rules_dir', '')
        if user_dir:
            search_paths = [os.path.expanduser(user_dir)] + search_paths

        for base in search_paths:
            path = os.path.join(base, name)
            if os.path.exists(path):
                return path
        return None

    def _request_rule_download(self, name: str) -> str | None:
        """Request UI thread to download a rule file. Blocks until done."""
        self._rule_download_event.clear()
        self.rule_download_needed.emit()

        # Wait for UI thread to set the event (download complete or cancelled)
        self._rule_download_event.wait()

        if self._cancelled:
            return None

        return self._rule_download_path

    def set_rule_download_result(self, path: str | None):
        """Called from UI thread after rule download completes."""
        self._rule_download_path = path
        self._rule_download_event.set()

    # ── Cancel / cleanup ───────────────────────────────────────────

    def cancel(self):
        self._cancelled = True
        self._rule_download_event.set()  # Unblock if waiting for download
        if self._process:
            try:
                self._process.terminate()
            except OSError:
                pass

    def _cleanup(self):
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except OSError:
                pass
