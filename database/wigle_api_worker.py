"""QThread worker for batch WiGLE API BSSID lookups."""

from PyQt6.QtCore import QThread, pyqtSignal

from database.wigle_api import WigleApiClient, WigleResult


class WigleApiWorker(QThread):
    """Background worker that looks up a list of BSSIDs via WiGLE API.

    Skips cached entries automatically. Emits progress and per-result signals.
    Stoppable via stop().
    """

    progress = pyqtSignal(int, int, str)        # current, total, bssid
    result_ready = pyqtSignal(object)            # WigleResult
    finished = pyqtSignal(int, int)              # looked_up, found

    def __init__(self, bssids: list[str], parent=None):
        super().__init__(parent)
        self._bssids = list(bssids)
        self._stopped = False

    def stop(self):
        self._stopped = True

    def run(self):
        client = WigleApiClient()
        total = len(self._bssids)
        looked_up = 0
        found = 0

        for i, bssid in enumerate(self._bssids):
            if self._stopped:
                break

            self.progress.emit(i + 1, total, bssid)

            if client.is_cached(bssid):
                result = client.get_cached(bssid)
            else:
                result = client.lookup_bssid(bssid)
                looked_up += 1

            if result and result.found:
                found += 1

            if result:
                self.result_ready.emit(result)

        self.finished.emit(looked_up, found)
