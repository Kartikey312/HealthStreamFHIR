"""
Excel copy of the PreAuth claim tables: one sheet per table, named after the
table, with the table's own column names as the header row and every stored
row beneath it - a straight copy of what is in the database, in the SP's
result-set order (shared/preauth_tables.TABLE_ORDER).
"""
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from typing import Any

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from sqlalchemy import text

from .preauth_tables import TABLE_ORDER, ensure_preauth_schema

EXCEL_CELL_LIMIT = 32767  # Excel rejects longer cell text (base64 attachments can exceed it)
MAX_COLUMN_WIDTH = 60


def _cell_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        value = ILLEGAL_CHARACTERS_RE.sub("", value)
        if len(value) > EXCEL_CELL_LIMIT:
            keep = EXCEL_CELL_LIMIT - 40
            value = f"{value[:keep]}...[truncated, {len(value)} chars]"
    return value


def build_preauth_workbook(engine) -> bytes:
    """Read every preauth table (creating any that is missing) and return the .xlsx file's bytes"""
    ensure_preauth_schema(engine)

    workbook = Workbook()
    workbook.remove(workbook.active)
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="305496")
    quote = engine.dialect.identifier_preparer.quote

    with engine.connect() as conn:
        for name in TABLE_ORDER:
            # SELECT * so the sheet mirrors the table exactly as it is in the database
            result = conn.execute(text(f"SELECT * FROM {quote(name)} ORDER BY 1"))
            columns = list(result.keys())
            sheet = workbook.create_sheet(title=name)
            sheet.append(columns)
            widths = [len(c) for c in columns]

            for row in result:
                values = [_cell_value(v) for v in row]
                sheet.append(values)
                for i, v in enumerate(values):
                    if isinstance(v, (datetime, date)):
                        sheet.cell(row=sheet.max_row, column=i + 1).number_format = "yyyy-mm-dd hh:mm:ss"
                    widths[i] = max(widths[i], len(str(v)) if v is not None else 0)

            for cell in sheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(vertical="center")
            for i, width in enumerate(widths, start=1):
                sheet.column_dimensions[get_column_letter(i)].width = min(width + 2, MAX_COLUMN_WIDTH)
            sheet.freeze_panes = "A2"

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()
