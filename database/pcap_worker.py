"""Background thread for parsing PCAP files."""

from PyQt6.QtCore import QThread, pyqtSignal


class PcapParseWorker(QThread):
    """Background worker for streaming PCAP parsing."""

    progress = pyqtSignal(int, int, dict)  # bytes_read, total_bytes, stats
    status = pyqtSignal(str)               # human-readable status text
    finished = pyqtSignal(bool, str)       # success, error_message

    def __init__(self, pcap_reader, file_path: str):
        super().__init__()
        self.pcap_reader = pcap_reader
        self.file_path = file_path
        self._cancelled = False

    def run(self):
        try:
            self.status.emit("Opening PCAP file...")
            self.pcap_reader.open_database(self.file_path)

            self.status.emit("Parsing packets...")
            completed = self.pcap_reader.parse(
                progress_callback=self._on_progress,
                cancel_check=lambda: self._cancelled,
            )

            if completed:
                self.finished.emit(True, "")
            else:
                self.finished.emit(True, "cancelled")
        except Exception as e:
            self.finished.emit(False, str(e))

    def _on_progress(self, bytes_read: int, total_bytes: int, stats: dict):
        self.progress.emit(bytes_read, total_bytes, stats)

    def cancel(self):
        self._cancelled = True
