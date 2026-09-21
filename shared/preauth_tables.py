"""
The 11 PreAuth claim tables the SQL Server SP USP_Get_PreAuthClaimsDetails
reads (message_header, claim, claim_related, claim_request_insurance,
diagnosis, care_team, supporting_info, claim_request_item,
claim_request_detail, coverage_class, claim_request_encounter), as the schema
of record for the incoming-claim flow.

The SP is only a column reference - it is never executed. Where it names the
columns (claim, supporting_info, claim_request_item, claim_request_detail,
diagnosis via its temp table) they are used as-is. It reads message_header,
claim_related, claim_request_insurance, care_team, coverage_class and
claim_request_encounter with SELECT *, so their columns are not visible in it;
those tables keep the columns init-db.sql already gave them, plus the columns
marked "inferred" below, taken from what a Dhamani Claim Bundle carries.

ensure_preauth_schema() creates whatever is missing and only ever ADDS
columns to tables that already exist (init-db.sql created several of them as
id + join-key stubs) - it never drops or alters anything.
"""
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List

from sqlalchemy import (
    MetaData, Table, Column, String, Integer, DateTime, Boolean, Numeric, Text, ForeignKey,
    delete, insert, inspect, select, text,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)

metadata = MetaData()

LONGTEXT = Text().with_variant(mysql.LONGTEXT(), "mysql")


def _id() -> Column:
    return Column("id", String(200), primary_key=True)


def _s(name: str, length: int = 255) -> Column:
    return Column(name, String(length), nullable=True)


def _n(name: str) -> Column:
    return Column(name, Numeric(18, 3), nullable=True)


def _i(name: str) -> Column:
    return Column(name, Integer, nullable=True)


def _d(name: str) -> Column:
    return Column(name, DateTime, nullable=True)


def _b(name: str) -> Column:
    return Column(name, Boolean, nullable=True)


def _claim_fk(name: str = "claim_id", nullable: bool = False) -> Column:
    return Column(name, String(200), ForeignKey("claim.id"), nullable=nullable, index=True)


message_header = Table(
    "message_header", metadata,
    _id(), _s("destination_receiver_identifier", 200), _s("event_coding", 100), _s("focus", 500),
    _s("response_code", 50), _s("response_identifier", 200), _s("sender_identifier", 200),
)

claim = Table(
    "claim", metadata,
    _id(), _d("accident_date"), _s("accident_location_adress", 500), _s("accident_type", 100),
    _d("billable_end"), _d("billable_start"), _s("bundle_id", 200), _b("consumed"), _d("created"),
    _s("diagnosis_related_group", 100), _s("eligibility_off_line", 50), _d("eligibility_off_line_date"),
    _s("eligibility_response_identifier", 200), _s("encounter_class", 100), _d("encounter_end"),
    _s("encounter_identifier", 200), _d("encounter_start"), _s("facility"), _s("identifier", 200),
    _s("identifier_system"), _d("inserted_on"), _d("inserted_on_date_time"), _s("insurer_identifier", 100),
    _s("insurer_identifier_system"), _s("khatm", 50), _b("new_born"), _s("patient_identifier", 100),
    _s("patient_identifier_system"), _s("patient_identifier_type", 50), _s("payee_party", 100),
    _s("payee_type", 100), _s("prescriber_identifier", 100), _s("prescription_identifier", 100),
    _s("priority", 50), _s("provider_identifier", 100), _s("referral", 200), _s("resource_type", 100),
    _s("status", 50), _s("sub_type", 100), _n("total"), _s("type", 100), _s("use", 50),
    _s("message_header_id", 200), _s("provider_name"), _s("insurer_name"), _d("patient_birth_date"),
    _s("patient_gender", 20), _s("patient_name"), _s("patient_telecom", 100), _s("order_category", 100),
    _s("order_reference", 200),
)

claim_related = Table(
    "claim_related", metadata,
    _id(), _s("claim_identifier", 200), _s("relationship", 100), _claim_fk(),
)

claim_request_insurance = Table(
    "claim_request_insurance", metadata,
    _id(), _s("coverage_identifier", 200), _s("coverage_subscriber_identifier", 200), _b("focal"),
    _i("sequence"), _claim_fk(), _s("coverage_subscriber_system"), _s("coverage_beneficiary_system"),
)

diagnosis = Table(
    "diagnosis", metadata,
    _id(), _claim_fk(),
    _s("diagnosis_admission"), _s("diagnosis_codeable_concept"), _s("diagnosis_type"), _i("sequence"),
)

care_team = Table(
    "care_team", metadata,
    _id(), _claim_fk(),
    # inferred - the SP reads this table with SELECT *
    _i("sequence"), _s("provider_identifier", 200), _s("provider_name"), _s("role", 100), _s("qualification", 100),
)

supporting_info = Table(
    "supporting_info", metadata,
    _id(), _s("category", 100), _s("code", 100), _s("reason", 200), _i("sequence"),
    _d("timing_date"), _d("timing_end"), _d("timing_start"), _s("value_attachment_content_type", 100),
    Column("value_attachment_data", LONGTEXT, nullable=True), _s("value_attachment_title"),
    _s("value_attachment_url", 500), _b("value_boolean"), _n("value_quantity"), _s("value_string", 500),
    _claim_fk(), _s("supporting_info_code", 100), _s("supporting_info_display"), _s("display"),
    Column("text", LONGTEXT, nullable=True),
)

claim_request_item = Table(
    "claim_request_item", metadata,
    _id(), _s("batch_number", 100), _s("body_site", 100), _i("care_team_sequence"), _i("diagnosis_sequence"),
    _d("expiry_date"), _n("factor"), _i("information_sequence"), _n("net"), _b("package_extenstion"),
    _n("patient_share"), _n("payer_share"), _s("product_or_service_code", 100), _s("product_or_service_system"),
    _n("quantity"), _i("sequence"), _s("serial_number", 100), _d("serviced_date"), _d("serviced_end"),
    _d("serviced_start"), _s("sub_site", 100), _n("tax"), _b("teleconsultation"), _n("unit_price"),
    _claim_fk(), _s("product_or_service_description", 500), _s("payer_code", 100), _s("payer_code_display"),
    _s("payer_code_system"),
)

claim_request_detail = Table(
    "claim_request_detail", metadata,
    _id(), _n("net"), _s("product_or_service_code", 100), _s("product_or_service_system"), _n("quantity"),
    _i("sequence"), _n("tax"), _n("unit_price"),
    Column("item_id", String(200), ForeignKey("claim_request_item.id"), nullable=False, index=True),
    _s("product_or_service_description", 500),
)

coverage_class = Table(
    "coverage_class", metadata,
    _id(),
    Column("insurance_id", String(200), ForeignKey("claim_request_insurance.id"), nullable=False, index=True),
    # claim_id is read by the SP (coverage_class.claim_id is null); the rest is inferred
    _s("claim_id", 200), _s("type", 100), _s("value", 200), _s("name"),
)

claim_request_encounter = Table(
    "claim_request_encounter", metadata,
    _id(), Column("identifier", String(200), nullable=False, index=True),
    # inferred - the SP reads this table with SELECT *
    _s("status", 50), _s("class_code", 50), _s("class_display", 100), _s("patient_identifier", 100),
    _d("period_start"), _d("period_end"), _s("service_provider_identifier", 100),
)

# Insert order (parents before children) - also the SP's result-set order, so
# it doubles as the Excel sheet order.
TABLE_ORDER: List[str] = [
    "message_header", "claim", "claim_related", "claim_request_insurance", "diagnosis", "care_team",
    "supporting_info", "claim_request_item", "claim_request_detail", "coverage_class", "claim_request_encounter",
]


def ensure_preauth_schema(engine) -> None:
    """Create any missing preauth table and add any missing column to existing ones. Additive only."""
    metadata.create_all(engine, checkfirst=True)

    inspector = inspect(engine)
    quote = engine.dialect.identifier_preparer.quote
    for table in metadata.sorted_tables:
        existing = {c["name"] for c in inspector.get_columns(table.name)}
        for column in table.columns:
            if column.name in existing:
                continue
            ddl = f"ALTER TABLE {quote(table.name)} ADD COLUMN {quote(column.name)} {column.type.compile(dialect=engine.dialect)} NULL"
            try:
                with engine.begin() as conn:
                    conn.execute(text(ddl))
                logger.info(f"✅ Added column {table.name}.{column.name}")
            except OperationalError as e:
                # another service starting at the same moment added it first
                if "uplicate column" not in str(e):
                    raise


def _coerce(column: Column, value: Any) -> Any:
    """JSON-safe value (ISO date string, float) -> the type its column wants"""
    if value is None:
        return None
    if isinstance(column.type, DateTime) and isinstance(value, str):
        return datetime.fromisoformat(value)
    if isinstance(column.type, Numeric) and not isinstance(value, Decimal):
        return Decimal(str(value))
    if isinstance(column.type, Boolean):
        return bool(value)
    return value


def store_preauth_tables(engine, tables: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    """
    Replace whatever is stored for the claim in tables["claim"] with these rows,
    in one transaction. Re-sending the same claim overwrites it instead of
    duplicating it. Returns {table: rows written}.
    """
    claim_rows = tables.get("claim") or []
    if not claim_rows:
        raise ValueError("No claim row to store")
    claim_id = claim_rows[0]["id"]

    with engine.begin() as conn:
        # children first (FK order), then the claim itself
        item_ids = select(claim_request_item.c.id).where(claim_request_item.c.claim_id == claim_id)
        insurance_ids = select(claim_request_insurance.c.id).where(claim_request_insurance.c.claim_id == claim_id)
        conn.execute(delete(claim_request_detail).where(claim_request_detail.c.item_id.in_(item_ids)))
        conn.execute(delete(coverage_class).where(coverage_class.c.insurance_id.in_(insurance_ids)))
        for table in (claim_request_item, supporting_info, care_team, diagnosis, claim_request_insurance, claim_related):
            conn.execute(delete(table).where(table.c.claim_id == claim_id))
        conn.execute(delete(claim).where(claim.c.id == claim_id))
        # shared, keyed by their own natural id rather than by claim
        for name in ("message_header", "claim_request_encounter"):
            table = metadata.tables[name]
            for row in tables.get(name) or []:
                conn.execute(delete(table).where(table.c.id == row["id"]))

        written: Dict[str, int] = {}
        for name in TABLE_ORDER:
            table = metadata.tables[name]
            rows = []
            for row in tables.get(name) or []:
                unknown = set(row) - set(table.c.keys())
                if unknown:
                    raise ValueError(f"{name}: unknown column(s) {sorted(unknown)}")
                rows.append({c.name: _coerce(c, row.get(c.name)) for c in table.columns})
            if rows:
                conn.execute(insert(table), rows)
            written[name] = len(rows)
    return written
