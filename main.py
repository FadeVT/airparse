#!/usr/bin/env python3
"""
Kismet Database GUI Reader

A cross-platform desktop application for reading and visualizing
Kismet wireless network capture data from .kismet database files.
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
    app.setApplicationName("Kismet Database Reader")
    app.setApplicationVersion("1.0.0")
    app.setOrganizationName("KismetGUI")

    # Create and show main window
    window = MainWindow()
    window.show()

    # Run event loop
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
