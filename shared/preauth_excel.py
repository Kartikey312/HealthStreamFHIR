"""
Excel copy of the PreAuth claim tables for GET /api/v1/preauth/export/excel:
one sheet per table with every stored row, in the SP's result-set order. The
layout itself lives in table_export, shared with the workflow Excel Export node.
"""
from .table_export import build_endpoint_workbook, workbook_bytes


def build_preauth_workbook(engine) -> bytes:
    """Read every preauth table (creating any that is missing) and return the .xlsx file's bytes"""
    return workbook_bytes(build_endpoint_workbook(engine, "preauth-claim", scope="all-rows"))
