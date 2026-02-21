#!/usr/bin/env python3
"""
AirParse — Wireless Capture Analyzer

A cross-platform desktop application for analyzing wireless capture data.
Supports Kismet .kismet databases and PCAP/PCAPNG files with hashcat
WPA cracking, interactive maps, and multi-format export.
"""

import sys
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt

from ui.main_window import MainWindow


def main():
    """Application entry point."""
    # Enable high DPI scaling
    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )

    # Create application
    app = QApplication(sys.argv)
    app.setApplicationName("AirParse")
    app.setApplicationVersion("2.0.0")
    app.setOrganizationName("AirParse")

    # Create and show main window
    window = MainWindow()
    window.show()

    # Run event loop
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
