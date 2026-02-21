#!/usr/bin/env bash
# AirParse — Setup Script
# Detects distro and installs dependencies, creates venv, and desktop entry.

set -euo pipefail

APP_NAME="airparse"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${APP_DIR}/venv"
DESKTOP_FILE="${HOME}/.local/share/applications/${APP_NAME}.desktop"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# --- Detect distro ---
detect_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        case "$ID" in
            arch|cachyos|garuda|endeavouros|manjaro|artix)
                echo "arch"
                ;;
            debian|ubuntu|linuxmint|pop|zorin|elementary)
                echo "debian"
                ;;
            fedora|rhel|centos|rocky|alma)
                echo "fedora"
                ;;
            opensuse*|suse*)
                echo "suse"
                ;;
            *)
                # Check ID_LIKE for derivatives
                case "${ID_LIKE:-}" in
                    *arch*) echo "arch" ;;
                    *debian*|*ubuntu*) echo "debian" ;;
                    *fedora*|*rhel*) echo "fedora" ;;
                    *) echo "unknown" ;;
                esac
                ;;
        esac
    else
        echo "unknown"
    fi
}

# --- Install system packages ---
install_system_deps() {
    local distro="$1"

    info "Detected distro family: ${distro}"

    case "$distro" in
        arch)
            info "Installing system dependencies via pacman..."
            sudo pacman -S --needed --noconfirm \
                python python-pip python-virtualenv \
                qt6-base qt6-webengine \
                sqlite \
                wireshark-cli \
                hashcat hcxtools \
                2>/dev/null || true
            ;;
        debian)
            info "Installing system dependencies via apt..."
            sudo apt-get update -qq
            sudo apt-get install -y \
                python3 python3-pip python3-venv \
                python3-pyqt6 python3-pyqt6.qtwebengine \
                sqlite3 \
                tshark \
                2>/dev/null || true
            ;;
        fedora)
            info "Installing system dependencies via dnf..."
            sudo dnf install -y \
                python3 python3-pip python3-virtualenv \
                python3-qt6 python3-qt6-webengine \
                sqlite \
                wireshark-cli \
                2>/dev/null || true
            ;;
        suse)
            info "Installing system dependencies via zypper..."
            sudo zypper install -y \
                python3 python3-pip python3-virtualenv \
                python3-qt6 \
                sqlite3 \
                wireshark \
                2>/dev/null || true
            ;;
        *)
            warn "Unknown distro. Please install manually:"
            warn "  - Python 3.10+, pip, venv"
            warn "  - Qt6 (PyQt6)"
            warn "  - sqlite3"
            warn "  - tshark / wireshark-cli (optional, for PCAP repair)"
            ;;
    esac
}

# --- Create virtual environment ---
setup_venv() {
    if [ -d "$VENV_DIR" ]; then
        info "Virtual environment already exists at ${VENV_DIR}"
    else
        info "Creating virtual environment..."
        python3 -m venv "$VENV_DIR"
        ok "Virtual environment created"
    fi

    info "Installing Python dependencies..."
    "${VENV_DIR}/bin/pip" install --quiet --upgrade pip
    "${VENV_DIR}/bin/pip" install --quiet -r "${APP_DIR}/requirements.txt"
    ok "Python dependencies installed"
}

# --- Create desktop entry ---
create_desktop_entry() {
    info "Creating desktop entry..."

    mkdir -p "$(dirname "$DESKTOP_FILE")"

    cat > "$DESKTOP_FILE" << EOF
[Desktop Entry]
Type=Application
Name=AirParse
Comment=Wireless capture analyzer (Kismet DB, PCAP, hashcat cracking)
Exec=${VENV_DIR}/bin/python3 ${APP_DIR}/main.py
Path=${APP_DIR}
Terminal=false
Categories=Network;Security;Utility;
Keywords=kismet;wifi;wireless;pcap;wireshark;wardriving;
EOF

    chmod +x "$DESKTOP_FILE"

    # Update desktop database if available
    if command -v update-desktop-database &>/dev/null; then
        update-desktop-database ~/.local/share/applications/ 2>/dev/null || true
    fi

    ok "Desktop entry created: ${DESKTOP_FILE}"
}

# --- Verify installation ---
verify() {
    info "Verifying installation..."
    local ok_count=0
    local total=0

    # Check Python
    total=$((total + 1))
    if "${VENV_DIR}/bin/python3" -c "print('Python OK')" &>/dev/null; then
        ok "Python 3"
        ok_count=$((ok_count + 1))
    else
        error "Python 3 not working"
    fi

    # Check PyQt6
    total=$((total + 1))
    if "${VENV_DIR}/bin/python3" -c "from PyQt6.QtWidgets import QApplication; print('PyQt6 OK')" &>/dev/null; then
        ok "PyQt6"
        ok_count=$((ok_count + 1))
    else
        error "PyQt6 import failed"
    fi

    # Check dpkt
    total=$((total + 1))
    if "${VENV_DIR}/bin/python3" -c "import dpkt; print('dpkt OK')" &>/dev/null; then
        ok "dpkt (PCAP parser)"
        ok_count=$((ok_count + 1))
    else
        error "dpkt not installed"
    fi

    # Check sqlite3
    total=$((total + 1))
    if command -v sqlite3 &>/dev/null; then
        ok "sqlite3 (for DB repair)"
        ok_count=$((ok_count + 1))
    else
        warn "sqlite3 not found (optional, for DB repair)"
    fi

    # Check tshark
    total=$((total + 1))
    if command -v tshark &>/dev/null; then
        ok "tshark (for PCAP repair)"
        ok_count=$((ok_count + 1))
    else
        warn "tshark not found (optional, for PCAP repair)"
    fi

    echo ""
    info "${ok_count}/${total} checks passed"
}

# --- Main ---
main() {
    echo ""
    echo "  AirParse — Setup"
    echo "  ================="
    echo ""

    local distro
    distro=$(detect_distro)

    # Step 1: System deps
    if [ "$distro" != "unknown" ]; then
        read -rp "Install system dependencies via package manager? [Y/n] " yn
        case "${yn:-Y}" in
            [Yy]*|"") install_system_deps "$distro" ;;
            *) info "Skipping system dependencies" ;;
        esac
    else
        warn "Could not detect distro. Skipping system package installation."
    fi

    echo ""

    # Step 2: Python venv
    setup_venv

    echo ""

    # Step 3: Desktop entry
    read -rp "Create desktop application entry? [Y/n] " yn
    case "${yn:-Y}" in
        [Yy]*|"") create_desktop_entry ;;
        *) info "Skipping desktop entry" ;;
    esac

    echo ""

    # Step 4: Verify
    verify

    echo ""
    ok "Setup complete!"
    echo ""
    info "To run: ${VENV_DIR}/bin/python3 ${APP_DIR}/main.py"
    echo ""
}

main "$@"
