"""PDF report generation for Kismet data."""

from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        PageBreak, Image
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False


class PDFExporter:
    """Generate PDF reports from Kismet data."""

    def __init__(self):
        if not HAS_REPORTLAB:
            raise ImportError("reportlab is required for PDF export. Install with: pip install reportlab")

        self.styles = getSampleStyleSheet()
        self._setup_styles()

    def _setup_styles(self):
        """Set up custom paragraph styles."""
        self.styles.add(ParagraphStyle(
            name='Title',
            parent=self.styles['Heading1'],
            fontSize=24,
            alignment=TA_CENTER,
            spaceAfter=30
        ))

        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceBefore=20,
            spaceAfter=10
        ))

        self.styles.add(ParagraphStyle(
            name='SubHeader',
            parent=self.styles['Heading3'],
            fontSize=12,
            spaceBefore=15,
            spaceAfter=5
        ))

        self.styles.add(ParagraphStyle(
            name='BodyText',
            parent=self.styles['Normal'],
            fontSize=10,
            spaceAfter=6
        ))

        self.styles.add(ParagraphStyle(
            name='TableHeader',
            parent=self.styles['Normal'],
            fontSize=9,
            textColor=colors.white
        ))

    def generate_report(self, db_reader, output_path: str,
                        include_devices: bool = True,
                        include_networks: bool = True,
                        include_alerts: bool = True,
                        max_table_rows: int = 50) -> bool:
        """
        Generate a comprehensive PDF report.

        Args:
            db_reader: KismetDBReader instance
            output_path: Path to output PDF file
            include_devices: Include device tables
            include_networks: Include network/SSID table
            include_alerts: Include alerts table
            max_table_rows: Maximum rows per table

        Returns:
            True if successful
        """
        if not HAS_REPORTLAB:
            return False

        try:
            doc = SimpleDocTemplate(
                output_path,
                pagesize=letter,
                rightMargin=0.5*inch,
                leftMargin=0.5*inch,
                topMargin=0.5*inch,
                bottomMargin=0.5*inch
            )

            story = []

            # Title
            story.append(Paragraph("Kismet Capture Report", self.styles['Title']))
            story.append(Paragraph(
                f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                self.styles['BodyText']
            ))
            story.append(Spacer(1, 20))

            # Summary section
            summary = db_reader.get_device_summary()
            story.extend(self._create_summary_section(summary, db_reader))

            # Device sections
            if include_devices:
                story.append(PageBreak())
                story.extend(self._create_devices_section(db_reader, max_table_rows))

            # Networks section
            if include_networks:
                story.append(PageBreak())
                story.extend(self._create_networks_section(db_reader, max_table_rows))

            # Alerts section
            if include_alerts:
                alerts_df = db_reader.get_alerts()
                if not alerts_df.empty:
                    story.append(PageBreak())
                    story.extend(self._create_alerts_section(alerts_df, max_table_rows))

            # Build PDF
            doc.build(story)
            return True

        except Exception as e:
            print(f"PDF export error: {e}")
            return False

    def _create_summary_section(self, summary: dict, db_reader) -> list:
        """Create the summary section."""
        elements = []

        elements.append(Paragraph("Executive Summary", self.styles['SectionHeader']))

        # Database info
        db_info = db_reader.get_database_info()
        elements.append(Paragraph(f"<b>Database:</b> {db_info.get('filename', 'Unknown')}", self.styles['BodyText']))
        elements.append(Paragraph(f"<b>Size:</b> {db_info.get('size_bytes', 0) / 1024 / 1024:.2f} MB", self.styles['BodyText']))

        # Time range
        time_range = summary.get('time_range', {})
        if time_range.get('earliest'):
            elements.append(Paragraph(f"<b>First Capture:</b> {time_range['earliest']}", self.styles['BodyText']))
            elements.append(Paragraph(f"<b>Last Capture:</b> {time_range['latest']}", self.styles['BodyText']))

        elements.append(Spacer(1, 15))

        # Device counts table
        elements.append(Paragraph("Device Summary", self.styles['SubHeader']))

        device_data = [['Device Type', 'Count']]
        device_data.append(['Total Devices', str(summary.get('total_devices', 0))])

        for phy_type, count in summary.get('by_phy_type', {}).items():
            device_data.append([phy_type, str(count)])

        # Add specific counts
        ap_df = db_reader.get_access_points()
        client_df = db_reader.get_clients()
        device_data.append(['Wi-Fi Access Points', str(len(ap_df))])
        device_data.append(['Wi-Fi Clients', str(len(client_df))])

        table = Table(device_data, colWidths=[3*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498db')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
            ('GRID', (0, 0), (-1, -1), 1, colors.white),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
        ]))
        elements.append(table)

        # Geographic info
        geo_bounds = summary.get('geographic_bounds', {})
        if geo_bounds.get('min_lat'):
            elements.append(Spacer(1, 15))
            elements.append(Paragraph("Geographic Coverage", self.styles['SubHeader']))
            elements.append(Paragraph(
                f"<b>Latitude:</b> {geo_bounds.get('min_lat', 0):.6f} to {geo_bounds.get('max_lat', 0):.6f}",
                self.styles['BodyText']
            ))
            elements.append(Paragraph(
                f"<b>Longitude:</b> {geo_bounds.get('min_lon', 0):.6f} to {geo_bounds.get('max_lon', 0):.6f}",
                self.styles['BodyText']
            ))
            elements.append(Paragraph(
                f"<b>Devices with GPS:</b> {geo_bounds.get('unique_devices_with_gps', 0)}",
                self.styles['BodyText']
            ))

        return elements

    def _create_devices_section(self, db_reader, max_rows: int) -> list:
        """Create the devices section."""
        elements = []

        elements.append(Paragraph("Devices", self.styles['SectionHeader']))

        # Access Points
        ap_df = db_reader.get_access_points()
        if not ap_df.empty:
            elements.append(Paragraph(f"Access Points ({len(ap_df)} total)", self.styles['SubHeader']))
            elements.append(self._create_device_table(
                ap_df.head(max_rows),
                ['devmac', 'ssid', 'channel', 'strongest_signal', 'manufacturer']
            ))
            if len(ap_df) > max_rows:
                elements.append(Paragraph(f"<i>Showing first {max_rows} of {len(ap_df)} access points</i>", self.styles['BodyText']))

        # Clients
        client_df = db_reader.get_clients()
        if not client_df.empty:
            elements.append(Spacer(1, 20))
            elements.append(Paragraph(f"Clients ({len(client_df)} total)", self.styles['SubHeader']))
            elements.append(self._create_device_table(
                client_df.head(max_rows),
                ['client_mac', 'manufacturer', 'strongest_signal', 'first_time']
            ))
            if len(client_df) > max_rows:
                elements.append(Paragraph(f"<i>Showing first {max_rows} of {len(client_df)} clients</i>", self.styles['BodyText']))

        # Bluetooth
        bt_df = db_reader.get_bluetooth_devices()
        if not bt_df.empty:
            elements.append(Spacer(1, 20))
            elements.append(Paragraph(f"Bluetooth Devices ({len(bt_df)} total)", self.styles['SubHeader']))
            elements.append(self._create_device_table(
                bt_df.head(max_rows),
                ['devmac', 'name', 'manufacturer', 'strongest_signal']
            ))
            if len(bt_df) > max_rows:
                elements.append(Paragraph(f"<i>Showing first {max_rows} of {len(bt_df)} Bluetooth devices</i>", self.styles['BodyText']))

        return elements

    def _create_device_table(self, df: pd.DataFrame, columns: list) -> Table:
        """Create a formatted table from DataFrame."""
        # Filter to existing columns
        cols = [c for c in columns if c in df.columns]
        if not cols:
            cols = list(df.columns)[:5]

        # Prepare header
        header_map = {
            'devmac': 'MAC Address',
            'client_mac': 'MAC Address',
            'ssid': 'SSID',
            'channel': 'Channel',
            'strongest_signal': 'Signal (dBm)',
            'manufacturer': 'Manufacturer',
            'name': 'Name',
            'first_time': 'First Seen',
            'last_time': 'Last Seen'
        }

        headers = [header_map.get(c, c.replace('_', ' ').title()) for c in cols]
        data = [headers]

        # Add rows
        for _, row in df.iterrows():
            row_data = []
            for col in cols:
                val = row.get(col, '')
                if pd.isna(val):
                    val = '-'
                elif hasattr(val, 'strftime'):
                    val = val.strftime('%Y-%m-%d %H:%M')
                else:
                    val = str(val)[:30]  # Truncate long values
                row_data.append(val)
            data.append(row_data)

        # Calculate column widths
        col_width = 7.5 * inch / len(cols)
        col_widths = [col_width] * len(cols)

        table = Table(data, colWidths=col_widths)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f8f8')])
        ]))

        return table

    def _create_networks_section(self, db_reader, max_rows: int) -> list:
        """Create the networks section."""
        elements = []

        networks_df = db_reader.get_networks()
        if networks_df.empty:
            return elements

        elements.append(Paragraph(f"Networks / SSIDs ({len(networks_df)} total)", self.styles['SectionHeader']))

        # Prepare data
        data = [['SSID', 'AP Count']]
        for _, row in networks_df.head(max_rows).iterrows():
            ssid = row.get('ssid', '')
            if not ssid or ssid == '':
                ssid = '<Hidden>'
            data.append([str(ssid)[:40], str(row.get('ap_count', 0))])

        table = Table(data, colWidths=[5*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27ae60')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f8f8')])
        ]))
        elements.append(table)

        if len(networks_df) > max_rows:
            elements.append(Paragraph(f"<i>Showing first {max_rows} of {len(networks_df)} networks</i>", self.styles['BodyText']))

        return elements

    def _create_alerts_section(self, alerts_df: pd.DataFrame, max_rows: int) -> list:
        """Create the alerts section."""
        elements = []

        elements.append(Paragraph(f"Alerts ({len(alerts_df)} total)", self.styles['SectionHeader']))

        if alerts_df.empty:
            elements.append(Paragraph("No alerts recorded.", self.styles['BodyText']))
            return elements

        # Prepare data
        data = [['Time', 'Type', 'Device', 'Header']]
        for _, row in alerts_df.head(max_rows).iterrows():
            timestamp = row.get('timestamp', row.get('ts_sec', ''))
            if hasattr(timestamp, 'strftime'):
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M')
            data.append([
                str(timestamp)[:19],
                str(row.get('phyname', '-'))[:15],
                str(row.get('devmac', '-'))[:17],
                str(row.get('header', '-'))[:30]
            ])

        table = Table(data, colWidths=[1.5*inch, 1.2*inch, 1.8*inch, 2.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e74c3c')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff5f5')])
        ]))
        elements.append(table)

        if len(alerts_df) > max_rows:
            elements.append(Paragraph(f"<i>Showing first {max_rows} of {len(alerts_df)} alerts</i>", self.styles['BodyText']))

        return elements


def export_to_pdf(db_reader, output_path: str) -> bool:
    """
    Convenience function to generate PDF report.

    Args:
        db_reader: KismetDBReader instance
        output_path: Path to output file

    Returns:
        True if successful
    """
    if not HAS_REPORTLAB:
        print("reportlab is required for PDF export")
        return False

    exporter = PDFExporter()
    return exporter.generate_report(db_reader, output_path)
