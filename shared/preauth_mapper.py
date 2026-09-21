"""
Dhamani PreAuth Claim FHIR message Bundle -> rows for the 11 PreAuth tables
(shared/preauth_tables.py), as one JSON-safe dict {table: [row, ...]}. That
dict is the "JSON response" of the incoming-claim flow: it is what gets stored
and what is published to Kafka.

Checked against one real bundle (a professional preauthorization Claim).
Extensions that bundle does not carry - eligibility off-line, khatm, new-born,
order category/reference, payer code - are looked up by a URL keyword and stay
null when absent; confirm those keywords against real Dhamani examples.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def _first(items: Optional[List[Any]]) -> Dict[str, Any]:
    return items[0] if items else {}


def _code(concept: Optional[Dict[str, Any]]) -> Optional[str]:
    return _first((concept or {}).get("coding")).get("code")


def _dt(value: Optional[str]) -> Optional[str]:
    """ISO date/datetime -> 'YYYY-MM-DD HH:MM:SS' wall-clock (MySQL DATETIME has no offset)"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _ext(resource: Dict[str, Any], *keywords: str) -> Dict[str, Any]:
    for extension in resource.get("extension", []) or []:
        url = extension.get("url", "")
        if any(k in url for k in keywords):
            return extension
    return {}


def _ext_value(extension: Dict[str, Any]) -> Any:
    if "valueBoolean" in extension:
        return extension["valueBoolean"]
    if "valueString" in extension:
        return extension["valueString"]
    if "valueDateTime" in extension:
        return extension["valueDateTime"]
    return _code(extension.get("valueCodeableConcept"))


def _str(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _bool(value: Any) -> Optional[bool]:
    return None if value is None else bool(value)


def _money(extension: Dict[str, Any]) -> Optional[float]:
    return (extension.get("valueMoney") or {}).get("value")


def _resources(bundle: Dict[str, Any], resource_type: str) -> List[Dict[str, Any]]:
    return [e.get("resource", {}) for e in bundle.get("entry", []) or []
            if e.get("resource", {}).get("resourceType") == resource_type]


def _resolve(bundle: Dict[str, Any], reference: Optional[str], resource_type: str) -> Dict[str, Any]:
    """
    Find the entry a reference points at: full URL match first, then by the
    trailing id (some bundles differ in host casing, e.g. Dhamani.om vs dhamani.om).
    """
    if not reference:
        return {}
    ref = reference.lower()
    for entry in bundle.get("entry", []) or []:
        if entry.get("fullUrl", "").lower() == ref:
            return entry.get("resource", {})
    tail = reference.rstrip("/").split("/")[-1]
    return next((r for r in _resources(bundle, resource_type) if r.get("id") == tail), {})


def find_claim(bundle: Dict[str, Any]) -> Dict[str, Any]:
    return _first(_resources(bundle, "Claim"))


def claim_key(bundle: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """(claim id, patient identifier) - enough to track a claim before it is mapped"""
    claim = find_claim(bundle)
    patient = _resolve(bundle, (claim.get("patient") or {}).get("reference"), "Patient") \
        or _first(_resources(bundle, "Patient"))
    return claim.get("id"), _first(patient.get("identifier")).get("value") or patient.get("id")


def to_preauth_tables(bundle: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    claim = find_claim(bundle)
    if not claim:
        raise ValueError("Bundle has no Claim entry")
    claim_id = claim["id"]

    header = _first(_resources(bundle, "MessageHeader"))
    patient = _resolve(bundle, (claim.get("patient") or {}).get("reference"), "Patient") \
        or _first(_resources(bundle, "Patient"))
    provider = _resolve(bundle, (claim.get("provider") or {}).get("reference"), "Organization")
    insurer = _resolve(bundle, (claim.get("insurer") or {}).get("reference"), "Organization")
    patient_ident = _first(patient.get("identifier"))
    claim_ident = _first(claim.get("identifier"))

    encounter_ref = ((_ext(claim, "extension-encounter").get("valueReference")) or {}).get("reference")
    encounter = _resolve(bundle, encounter_ref, "Encounter")
    encounter_ident = _first(encounter.get("identifier")).get("value") or encounter.get("id") \
        or (encounter_ref.rstrip("/").split("/")[-1] if encounter_ref else None)

    prescriber = (_ext(claim, "extension-prescriber").get("valueReference")) or {}
    eligibility = (_ext(claim, "extension-eligibility-response").get("valueReference")) or {}
    accident = claim.get("accident") or {}
    billable = claim.get("billablePeriod") or {}
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    tables: Dict[str, List[Dict[str, Any]]] = {}

    tables["message_header"] = [{
        "id": header.get("id"),
        "destination_receiver_identifier": ((_first(header.get("destination")).get("receiver") or {})
                                            .get("identifier") or {}).get("value"),
        "event_coding": (header.get("eventCoding") or {}).get("code"),
        "focus": _first(header.get("focus")).get("reference"),
        "response_code": (header.get("response") or {}).get("code"),
        "response_identifier": (header.get("response") or {}).get("identifier"),
        "sender_identifier": ((header.get("sender") or {}).get("identifier") or {}).get("value"),
    }] if header.get("id") else []

    tables["claim"] = [{
        "id": claim_id,
        "accident_date": _dt(accident.get("date")),
        "accident_location_adress": (accident.get("locationAddress") or {}).get("text"),
        "accident_type": _code(accident.get("type")),
        "billable_end": _dt(billable.get("end")),
        "billable_start": _dt(billable.get("start")),
        "bundle_id": bundle.get("id"),
        "consumed": False,
        "created": _dt(claim.get("created")),
        "diagnosis_related_group": _code(_first(claim.get("diagnosis")).get("packageCode")),
        "eligibility_off_line": _str(_ext_value(_ext(claim, "off-line", "offline"))),
        "eligibility_off_line_date": None,
        "eligibility_response_identifier": (eligibility.get("identifier") or {}).get("value"),
        "encounter_class": (encounter.get("class") or {}).get("code"),
        "encounter_end": _dt((encounter.get("period") or {}).get("end")),
        "encounter_identifier": encounter_ident,
        "encounter_start": _dt((encounter.get("period") or {}).get("start")),
        "facility": (claim.get("facility") or {}).get("display"),
        "identifier": claim_ident.get("value"),
        "identifier_system": claim_ident.get("system"),
        "inserted_on": now,
        "inserted_on_date_time": now,
        "insurer_identifier": _first(insurer.get("identifier")).get("value"),
        "insurer_identifier_system": _first(insurer.get("identifier")).get("system"),
        "khatm": _str(_ext_value(_ext(claim, "khatm"))),
        "new_born": _bool(_ext_value(_ext(claim, "new-born", "newborn"))),
        "patient_identifier": patient_ident.get("value"),
        "patient_identifier_system": patient_ident.get("system"),
        "patient_identifier_type": _code(patient_ident.get("type")),
        "payee_party": ((claim.get("payee") or {}).get("party") or {}).get("reference"),
        "payee_type": _code((claim.get("payee") or {}).get("type")),
        "prescriber_identifier": (prescriber.get("identifier") or {}).get("value"),
        "prescription_identifier": ((claim.get("prescription") or {}).get("identifier") or {}).get("value"),
        "priority": _code(claim.get("priority")),
        "provider_identifier": _first(provider.get("identifier")).get("value"),
        "referral": (claim.get("referral") or {}).get("reference"),
        "resource_type": claim.get("resourceType"),
        "status": claim.get("status"),
        "sub_type": _code(claim.get("subType")),
        "total": (claim.get("total") or {}).get("value"),
        "type": _code(claim.get("type")),
        "use": claim.get("use"),
        "message_header_id": header.get("id"),
        "provider_name": provider.get("name"),
        "insurer_name": insurer.get("name"),
        "patient_birth_date": _dt(patient.get("birthDate")),
        "patient_gender": patient.get("gender"),
        "patient_name": _first(patient.get("name")).get("text"),
        "patient_telecom": _first(patient.get("telecom")).get("value"),
        "order_category": _str(_ext_value(_ext(claim, "order-category"))),
        "order_reference": _str(_ext_value(_ext(claim, "order-reference"))),
    }]

    tables["claim_related"] = []
    for n, related in enumerate(claim.get("related") or [], start=1):
        ref = related.get("claim") or {}
        tables["claim_related"].append({
            "id": f"{claim_id}-REL-{n}",
            "claim_identifier": (ref.get("identifier") or {}).get("value") or ref.get("reference"),
            "relationship": _code(related.get("relationship")),
            "claim_id": claim_id,
        })

    tables["claim_request_insurance"] = []
    tables["coverage_class"] = []
    for n, insurance in enumerate(claim.get("insurance") or [], start=1):
        seq = insurance.get("sequence") or n
        insurance_id = f"{claim_id}-INS-{seq}"
        coverage = _resolve(bundle, (insurance.get("coverage") or {}).get("reference"), "Coverage")
        subscriber = _resolve(bundle, (coverage.get("subscriber") or {}).get("reference"), "Patient") or patient
        beneficiary = _resolve(bundle, (coverage.get("beneficiary") or {}).get("reference"), "Patient") or patient
        tables["claim_request_insurance"].append({
            "id": insurance_id,
            "coverage_identifier": _first(coverage.get("identifier")).get("value"),
            "coverage_subscriber_identifier": _first(subscriber.get("identifier")).get("value"),
            "focal": insurance.get("focal"),
            "sequence": seq,
            "claim_id": claim_id,
            "coverage_subscriber_system": _first(subscriber.get("identifier")).get("system"),
            "coverage_beneficiary_system": _first(beneficiary.get("identifier")).get("system"),
        })
        for m, cls in enumerate(coverage.get("class") or [], start=1):
            tables["coverage_class"].append({
                "id": f"{insurance_id}-CLS-{m}",
                "insurance_id": insurance_id,
                "claim_id": None,
                "type": _code(cls.get("type")),
                "value": cls.get("value"),
                "name": cls.get("name"),
            })

    tables["diagnosis"] = []
    for n, dx in enumerate(claim.get("diagnosis") or [], start=1):
        seq = dx.get("sequence") or n
        tables["diagnosis"].append({
            "id": f"{claim_id}-DX-{seq}",
            "claim_id": claim_id,
            "diagnosis_admission": _code(dx.get("onAdmission")),
            "diagnosis_codeable_concept": _code(dx.get("diagnosisCodeableConcept")),
            "diagnosis_type": _code(_first(dx.get("type"))),
            "sequence": seq,
        })

    tables["care_team"] = []
    for n, member in enumerate(claim.get("careTeam") or [], start=1):
        seq = member.get("sequence") or n
        practitioner = _resolve(bundle, (member.get("provider") or {}).get("reference"), "Practitioner")
        tables["care_team"].append({
            "id": f"{claim_id}-CT-{seq}",
            "claim_id": claim_id,
            "sequence": seq,
            "provider_identifier": _first(practitioner.get("identifier")).get("value"),
            "provider_name": _first(practitioner.get("name")).get("text"),
            "role": _code(member.get("role")),
            "qualification": _code(member.get("qualification")),
        })

    tables["supporting_info"] = []
    for n, info in enumerate(claim.get("supportingInfo") or [], start=1):
        seq = info.get("sequence") or n
        attachment = info.get("valueAttachment") or {}
        timing = info.get("timingPeriod") or {}
        code = _first((info.get("code") or {}).get("coding"))
        reason = _first((info.get("reason") or {}).get("coding"))
        tables["supporting_info"].append({
            "id": f"{claim_id}-SI-{seq}",
            "category": _code(info.get("category")),
            "code": code.get("code"),
            "reason": reason.get("code"),
            "sequence": seq,
            "timing_date": _dt(info.get("timingDate")),
            "timing_end": _dt(timing.get("end")),
            "timing_start": _dt(timing.get("start")),
            "value_attachment_content_type": attachment.get("contentType"),
            "value_attachment_data": attachment.get("data"),
            "value_attachment_title": attachment.get("title"),
            "value_attachment_url": attachment.get("url"),
            "value_boolean": info.get("valueBoolean"),
            "value_quantity": (info.get("valueQuantity") or {}).get("value"),
            "value_string": info.get("valueString"),
            "claim_id": claim_id,
            "supporting_info_code": code.get("code"),
            "supporting_info_display": code.get("display"),
            "display": reason.get("display"),
            "text": (info.get("code") or {}).get("text"),
        })

    tables["claim_request_item"] = []
    tables["claim_request_detail"] = []
    for n, item in enumerate(claim.get("item") or [], start=1):
        seq = item.get("sequence") or n
        item_id = f"{claim_id}-ITEM-{seq}"
        product = _first((item.get("productOrService") or {}).get("coding"))
        period = item.get("servicedPeriod") or {}
        payer_code = _ext(item, "payer-code").get("valueCodeableConcept")
        payer_coding = _first((payer_code or {}).get("coding"))
        tables["claim_request_item"].append({
            "id": item_id,
            "batch_number": _str(_ext_value(_ext(item, "batch-number"))),
            "body_site": _code(item.get("bodySite")),
            "care_team_sequence": _first(item.get("careTeamSequence")) if item.get("careTeamSequence") else None,
            "diagnosis_sequence": _first(item.get("diagnosisSequence")) if item.get("diagnosisSequence") else None,
            "expiry_date": None,
            "factor": item.get("factor"),
            "information_sequence": _first(item.get("informationSequence")) if item.get("informationSequence") else None,
            "net": (item.get("net") or {}).get("value"),
            "package_extenstion": _ext(item, "extension-package").get("valueBoolean"),
            "patient_share": _money(_ext(item, "extension-patient-share")),
            "payer_share": _money(_ext(item, "extension-payer-share")),
            "product_or_service_code": product.get("code"),
            "product_or_service_system": product.get("system"),
            "quantity": (item.get("quantity") or {}).get("value"),
            "sequence": seq,
            "serial_number": None,
            "serviced_date": _dt(item.get("servicedDate")),
            "serviced_end": _dt(period.get("end")),
            "serviced_start": _dt(period.get("start")),
            "sub_site": _code(_first(item.get("subSite"))),
            "tax": _money(_ext(item, "extension-tax")),
            "teleconsultation": _ext(item, "teleconsultation").get("valueBoolean"),
            "unit_price": (item.get("unitPrice") or {}).get("value"),
            "claim_id": claim_id,
            "product_or_service_description": product.get("display"),
            "payer_code": payer_coding.get("code"),
            "payer_code_display": payer_coding.get("display"),
            "payer_code_system": payer_coding.get("system"),
        })
        for m, detail in enumerate(item.get("detail") or [], start=1):
            dseq = detail.get("sequence") or m
            dproduct = _first((detail.get("productOrService") or {}).get("coding"))
            tables["claim_request_detail"].append({
                "id": f"{item_id}-DET-{dseq}",
                "net": (detail.get("net") or {}).get("value"),
                "product_or_service_code": dproduct.get("code"),
                "product_or_service_system": dproduct.get("system"),
                "quantity": (detail.get("quantity") or {}).get("value"),
                "sequence": dseq,
                "tax": _money(_ext(detail, "extension-tax")),
                "unit_price": (detail.get("unitPrice") or {}).get("value"),
                "item_id": item_id,
                "product_or_service_description": dproduct.get("display"),
            })

    tables["claim_request_encounter"] = [{
        "id": encounter.get("id") or f"{claim_id}-ENC",
        "identifier": encounter_ident,
        "status": encounter.get("status"),
        "class_code": (encounter.get("class") or {}).get("code"),
        "class_display": (encounter.get("class") or {}).get("display"),
        "patient_identifier": patient_ident.get("value"),
        "period_start": _dt((encounter.get("period") or {}).get("start")),
        "period_end": _dt((encounter.get("period") or {}).get("end")),
        "service_provider_identifier": _first(provider.get("identifier")).get("value"),
    }] if encounter and encounter_ident else []

    return tables


# ---------------------------------------------------------------------------
# The other direction: table rows -> Dhamani Claim Bundle (the outgoing request)
# ---------------------------------------------------------------------------

_DHAMANI = "http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition"
_TERMINOLOGY = "http://dhamani.om/terminology/CodeSystem"
_CURRENCY = "OMR"


def _iso(value: Optional[str]) -> Optional[str]:
    """'2025-05-14 00:00:00' -> '2025-05-14'; other datetimes -> ISO 'T' form (the rows hold wall-clock time)"""
    if not value:
        return None
    return value[:10] if value.endswith(" 00:00:00") else value.replace(" ", "T")


def _coding(system: str, code: Optional[str], display: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if code is None:
        return None
    coding = {"system": system, "code": code}
    if display:
        coding["display"] = display
    return {"coding": [coding]}


def _drop_none(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _uuid(kind: str, value: str) -> str:
    import uuid
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{kind}:{value}"))


def to_fhir_preauth_bundle(tables: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Rows of the PreAuth tables ({table: [row, ...]}, the shape to_preauth_tables
    produces and the request endpoint accepts) -> a Dhamani Claim message
    Bundle. The inverse of to_preauth_tables: mapping a Bundle to rows and back
    gives the same rows.
    """
    import uuid
    from datetime import timezone, timedelta

    claim_rows = tables.get("claim") or []
    if not claim_rows:
        raise ValueError("No claim row to build a Bundle from")
    c = claim_rows[0]
    claim_id = c["id"]
    header_row = _first(tables.get("message_header"))
    encounter_row = _first(tables.get("claim_request_encounter"))

    provider_ident, insurer_ident = c.get("provider_identifier"), c.get("insurer_identifier")
    if not provider_ident or not insurer_ident:
        raise ValueError("claim needs provider_identifier and insurer_identifier")
    base = f"http://{provider_ident}.Dhamani.om"

    def url(kind: str, ident: str) -> str:
        return f"{base}/{kind}/{ident}"

    provider_org_id, insurer_org_id = _uuid("provider", provider_ident), _uuid("insurer", insurer_ident)
    provider_url, insurer_url = url("Organization", provider_org_id), url("Organization", insurer_org_id)
    patient_ident = c.get("patient_identifier")
    patient_url = url("Patient", patient_ident) if patient_ident else None
    claim_url = url("Claim", claim_id)
    header_id = header_row.get("id") or c.get("message_header_id") or str(uuid.uuid4())

    entries: List[Dict[str, Any]] = []

    def add(resource: Dict[str, Any], full_url: str) -> None:
        entries.append({"fullUrl": full_url, "resource": resource})

    # MessageHeader
    header = {
        "resourceType": "MessageHeader", "id": header_id,
        "meta": {"profile": [f"{_DHAMANI}/message-header|1.0.0"]},
        "eventCoding": {"system": f"{_TERMINOLOGY}/om-message-events",
                        "code": header_row.get("event_coding") or "priorauth-request"},
        "destination": [{
            "endpoint": f"http://{insurer_ident}.Dhamani.om/$process-message",
            "receiver": {"type": "Organization", "identifier": {
                "system": "http://dhamani.om/license/payer-license",
                "value": header_row.get("destination_receiver_identifier") or insurer_ident}},
        }],
        "sender": {"type": "Organization", "identifier": {
            "system": "http://dhamani.om/license/provider-license",
            "value": header_row.get("sender_identifier") or provider_ident}},
        "source": {"endpoint": base},
        "focus": [{"reference": claim_url}],
    }
    if header_row.get("response_identifier") or header_row.get("response_code"):
        header["response"] = _drop_none({"identifier": header_row.get("response_identifier"),
                                         "code": header_row.get("response_code")})
    add(header, f"urn:uuid:{header_id}")

    # Organizations
    def organization(org_id: str, profile: str, license_kind: str, type_code: str, ident: str,
                     system: Optional[str], name: Optional[str]) -> Dict[str, Any]:
        return _drop_none({
            "resourceType": "Organization", "id": org_id,
            "meta": {"profile": [f"{_DHAMANI}/{profile}|1.0.0"]},
            "identifier": [{"system": system or f"http://dhamani.om/license/{license_kind}", "value": ident}],
            "active": True,
            "type": [_coding(f"{_TERMINOLOGY}/organization-type", type_code)],
            "name": name,
        })
    add(organization(provider_org_id, "provider-organization", "provider-license", "prov", provider_ident,
                     None, c.get("provider_name")), provider_url)
    add(organization(insurer_org_id, "insurer-organization", "payer-license", "ins", insurer_ident,
                     c.get("insurer_identifier_system"), c.get("insurer_name")), insurer_url)

    # Patient (and a separate subscriber when the policy holder is someone else)
    def patient_resource(ident: str, system: Optional[str], type_code: Optional[str], row: bool) -> Dict[str, Any]:
        resource = {
            "resourceType": "Patient", "id": ident,
            "meta": {"profile": [f"{_DHAMANI}/patient|1.0.0"]},
            "identifier": [_drop_none({
                "type": _coding("http://terminology.hl7.org/CodeSystem/v2-0203", type_code,
                                "National unique individual identifier" if type_code == "NI" else None),
                "system": system, "value": ident})],
            "active": True,
        }
        if row:
            if c.get("patient_name"):
                resource["name"] = [{"use": "official", "text": c["patient_name"], "given": [c["patient_name"]]}]
            if c.get("patient_telecom"):
                resource["telecom"] = [{"system": "phone", "value": c["patient_telecom"]}]
            resource["gender"] = c.get("patient_gender")
            resource["birthDate"] = _iso(c.get("patient_birth_date"))
        return _drop_none(resource)
    if patient_ident:
        add(patient_resource(patient_ident, c.get("patient_identifier_system"), c.get("patient_identifier_type"), True),
            patient_url)

    # Coverage per insurance row, with its classes
    classes_by_insurance: Dict[str, List[Dict[str, Any]]] = {}
    for cls in tables.get("coverage_class") or []:
        classes_by_insurance.setdefault(cls.get("insurance_id"), []).append(cls)

    insurance = []
    for n, row in enumerate(tables.get("claim_request_insurance") or [], start=1):
        seq = row.get("sequence") or n
        coverage_ident = row.get("coverage_identifier") or f"{claim_id}-COV-{seq}"
        coverage_url = url("Coverage", coverage_ident)
        subscriber_ident = row.get("coverage_subscriber_identifier") or patient_ident
        subscriber_url = url("Patient", subscriber_ident) if subscriber_ident else patient_url
        if subscriber_ident and subscriber_ident != patient_ident:
            add(patient_resource(subscriber_ident, row.get("coverage_subscriber_system"), None, False), subscriber_url)
        coverage = _drop_none({
            "resourceType": "Coverage", "id": coverage_ident,
            "meta": {"profile": [f"{_DHAMANI}/coverage|1.0.0"]},
            "identifier": [{"value": row["coverage_identifier"]}] if row.get("coverage_identifier") else None,
            "status": "active",
            "subscriber": {"reference": subscriber_url} if subscriber_url else None,
            "beneficiary": {"reference": patient_url} if patient_url else None,
            "relationship": _coding("http://terminology.hl7.org/CodeSystem/subscriber-relationship", "self"),
            "payor": [{"reference": insurer_url}],
            "class": [_drop_none({
                "type": _coding("http://terminology.hl7.org/CodeSystem/coverage-class", k.get("type")),
                "value": k.get("value"), "name": k.get("name")})
                for k in classes_by_insurance.get(row.get("id"), [])] or None,
        })
        add(coverage, coverage_url)
        insurance.append(_drop_none({"sequence": seq, "focal": row.get("focal"), "coverage": {"reference": coverage_url}}))

    # Encounter
    encounter_url = None
    if encounter_row.get("id"):
        encounter_url = url("encounter", encounter_row["id"])
        add(_drop_none({
            "resourceType": "Encounter", "id": encounter_row["id"],
            "meta": {"profile": [f"{_DHAMANI}/encounter|1.0.0"]},
            "identifier": [{"system": f"{base}/identifier/Encounter", "value": encounter_row.get("identifier")}],
            "status": encounter_row.get("status"),
            "class": _drop_none({"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                                 "code": encounter_row.get("class_code"), "display": encounter_row.get("class_display")})
            if encounter_row.get("class_code") else None,
            "subject": {"reference": patient_url} if patient_url else None,
            "period": _drop_none({"start": _iso(encounter_row.get("period_start")),
                                  "end": _iso(encounter_row.get("period_end"))}) or None,
            "serviceProvider": {"reference": provider_url},
        }), encounter_url)
    elif c.get("encounter_identifier"):
        encounter_url = url("encounter", c["encounter_identifier"])

    # Practitioners + care team
    care_team, practitioner_names = [], {}
    for n, member in enumerate(tables.get("care_team") or [], start=1):
        seq = member.get("sequence") or n
        provider_ref = None
        if member.get("provider_identifier"):
            practitioner_id = _uuid("practitioner", member["provider_identifier"])
            provider_ref = url("Practitioner", practitioner_id)
            practitioner_names[member["provider_identifier"]] = member.get("provider_name")
            if not any(e["fullUrl"] == provider_ref for e in entries):
                add(_drop_none({
                    "resourceType": "Practitioner", "id": practitioner_id,
                    "meta": {"profile": [f"{_DHAMANI}/practitioner|1.0.0"]},
                    "identifier": [{
                        "type": _coding("http://terminology.hl7.org/CodeSystem/v2-0203", "MD", "Medical License"),
                        "system": "http://dhamani.om/license/practitioner-license",
                        "value": member["provider_identifier"]}],
                    "active": True,
                    "name": [{"use": "official", "text": member["provider_name"]}] if member.get("provider_name") else None,
                }), provider_ref)
        care_team.append(_drop_none({
            "sequence": seq,
            "provider": {"reference": provider_ref} if provider_ref else None,
            "role": _coding(f"{_TERMINOLOGY}/practitioner-role", member.get("role")),
            "qualification": _coding(f"{_TERMINOLOGY}/practice-codes", member.get("qualification")),
        }))

    # Supporting info
    supporting_info = []
    for n, si in enumerate(tables.get("supporting_info") or [], start=1):
        entry = _drop_none({
            "sequence": si.get("sequence") or n,
            "category": _coding(f"{_TERMINOLOGY}/claim-information-category", si.get("category"),
                                "Attachment" if si.get("category") == "attachment" else None),
            "code": _drop_none({"coding": [_drop_none({"code": si.get("supporting_info_code") or si.get("code"),
                                                        "display": si.get("supporting_info_display")})],
                                "text": si.get("text")}) if (si.get("supporting_info_code") or si.get("code")) else (
                    {"text": si["text"]} if si.get("text") else None),
            "reason": _coding(f"{_TERMINOLOGY}/info-reason", si.get("reason"), si.get("display")),
            "timingDate": _iso(si.get("timing_date")),
            "timingPeriod": _drop_none({"start": _iso(si.get("timing_start")), "end": _iso(si.get("timing_end"))}) or None,
            "valueBoolean": None if si.get("value_boolean") is None else bool(si["value_boolean"]),
            "valueQuantity": {"value": si["value_quantity"]} if si.get("value_quantity") is not None else None,
            "valueString": si.get("value_string"),
        })
        attachment = _drop_none({"contentType": si.get("value_attachment_content_type"),
                                 "data": si.get("value_attachment_data"), "title": si.get("value_attachment_title"),
                                 "url": si.get("value_attachment_url")})
        if attachment:
            entry["valueAttachment"] = attachment
        supporting_info.append(entry)

    # Diagnosis
    diagnosis = []
    for n, dx in enumerate(tables.get("diagnosis") or [], start=1):
        entry = _drop_none({
            "sequence": dx.get("sequence") or n,
            "diagnosisCodeableConcept": _coding("http://hl7.org/fhir/sid/icd-10-cm", dx.get("diagnosis_codeable_concept")),
            "type": [_coding(f"{_TERMINOLOGY}/diagnosis-type", dx.get("diagnosis_type"))] if dx.get("diagnosis_type") else None,
            "onAdmission": _coding("http://terminology.hl7.org/CodeSystem/ex-diagnosis-on-admission",
                                   dx.get("diagnosis_admission")),
        })
        if n == 1 and c.get("diagnosis_related_group"):
            entry["packageCode"] = _coding(f"{_TERMINOLOGY}/drg", c["diagnosis_related_group"])
        diagnosis.append(entry)

    # Items and their details
    details_by_item: Dict[str, List[Dict[str, Any]]] = {}
    for d in tables.get("claim_request_detail") or []:
        details_by_item.setdefault(d.get("item_id"), []).append(d)

    def money(value: Any) -> Optional[Dict[str, Any]]:
        return None if value is None else {"value": value, "currency": _CURRENCY}

    def ext(name: str, **value: Any) -> Optional[Dict[str, Any]]:
        return None if all(v is None for v in value.values()) else {"url": f"{_DHAMANI}/extension-{name}", **value}

    items = []
    for n, it in enumerate(tables.get("claim_request_item") or [], start=1):
        seq = it.get("sequence") or n
        payer_code = _drop_none({"system": it.get("payer_code_system"), "code": it.get("payer_code") or None,
                                 "display": it.get("payer_code_display") or None})
        extensions = [e for e in (
            ext("package", valueBoolean=None if it.get("package_extenstion") is None else bool(it["package_extenstion"])),
            ext("tax", valueMoney=money(it.get("tax"))),
            ext("patient-share", valueMoney=money(it.get("patient_share"))),
            ext("payer-share", valueMoney=money(it.get("payer_share"))),
            ext("teleconsultation", valueBoolean=None if it.get("teleconsultation") is None else bool(it["teleconsultation"])),
            ext("batch-number", valueString=it.get("batch_number")),
            ext("payer-code", valueCodeableConcept={"coding": [payer_code]}) if payer_code.get("code") else None,
        ) if e]
        item = _drop_none({
            "extension": extensions or None,
            "sequence": seq,
            "careTeamSequence": [it["care_team_sequence"]] if it.get("care_team_sequence") is not None else None,
            "diagnosisSequence": [it["diagnosis_sequence"]] if it.get("diagnosis_sequence") is not None else None,
            "informationSequence": [it["information_sequence"]] if it.get("information_sequence") is not None else None,
            "productOrService": {"coding": [_drop_none({
                "system": it.get("product_or_service_system"), "code": it.get("product_or_service_code"),
                "display": it.get("product_or_service_description")})]},
            "servicedDate": _iso(it.get("serviced_date")) if not (it.get("serviced_start") or it.get("serviced_end")) else None,
            "servicedPeriod": _drop_none({"start": _iso(it.get("serviced_start")), "end": _iso(it.get("serviced_end"))}) or None,
            "bodySite": _coding(f"{_TERMINOLOGY}/body-site", it.get("body_site")),
            "subSite": [_coding(f"{_TERMINOLOGY}/sub-site", it["sub_site"])] if it.get("sub_site") else None,
            "quantity": {"value": it["quantity"]} if it.get("quantity") is not None else None,
            "unitPrice": money(it.get("unit_price")),
            "factor": it.get("factor"),
            "net": money(it.get("net")),
        })
        item_details = []
        for m, d in enumerate(details_by_item.get(it.get("id"), []), start=1):
            item_details.append(_drop_none({
                "sequence": d.get("sequence") or m,
                "productOrService": {"coding": [_drop_none({
                    "system": d.get("product_or_service_system"), "code": d.get("product_or_service_code"),
                    "display": d.get("product_or_service_description")})]},
                "quantity": {"value": d["quantity"]} if d.get("quantity") is not None else None,
                "unitPrice": money(d.get("unit_price")), "net": money(d.get("net")),
                "extension": [e for e in (ext("tax", valueMoney=money(d.get("tax"))),) if e] or None,
            }))
        if item_details:
            item["detail"] = item_details
        items.append(item)

    # Claim
    claim_extensions = [e for e in (
        {"url": f"{_DHAMANI}/extension-encounter", "valueReference": {"reference": encounter_url}} if encounter_url else None,
        {"url": f"{_DHAMANI}/extension-eligibility-response",
         "valueReference": {"identifier": {"value": c["eligibility_response_identifier"]}}}
        if c.get("eligibility_response_identifier") else None,
        {"url": f"{_DHAMANI}/extension-prescriber", "valueReference": _drop_none({
            "type": "Practitioner",
            "identifier": {"system": "http://dhamani.om/license/practitioner-license", "value": c["prescriber_identifier"]},
            "display": practitioner_names.get(c["prescriber_identifier"])})} if c.get("prescriber_identifier") else None,
        ext("eligibility-off-line", valueString=c.get("eligibility_off_line")),
        ext("khatm", valueString=c.get("khatm")),
        ext("new-born", valueBoolean=None if c.get("new_born") is None else bool(c["new_born"])),
        ext("order-category", valueString=c.get("order_category")),
        ext("order-reference", valueString=c.get("order_reference")),
    ) if e]
    accident = _drop_none({
        "date": _iso(c.get("accident_date")), "type": _coding(f"{_TERMINOLOGY}/accident-type", c.get("accident_type")),
        "locationAddress": {"text": c["accident_location_adress"]} if c.get("accident_location_adress") else None})
    billable = _drop_none({"start": _iso(c.get("billable_start")), "end": _iso(c.get("billable_end"))})
    claim = _drop_none({
        "resourceType": "Claim", "id": claim_id,
        "meta": {"profile": [f"{_DHAMANI}/professional-priorauth|1.0.0"]} if c.get("use") == "preauthorization" else None,
        "extension": claim_extensions or None,
        "identifier": [_drop_none({"system": c.get("identifier_system"), "value": c.get("identifier") or claim_id})],
        "status": c.get("status") or "active",
        "type": _coding("http://terminology.hl7.org/CodeSystem/claim-type", c.get("type")),
        "subType": _coding(f"{_TERMINOLOGY}/claim-subtype", c.get("sub_type")),
        "use": c.get("use"),
        "patient": {"reference": patient_url} if patient_url else None,
        "billablePeriod": billable or None,
        "created": _iso(c.get("created")),
        "insurer": {"reference": insurer_url}, "provider": {"reference": provider_url},
        "priority": _coding("http://terminology.hl7.org/CodeSystem/processpriority", c.get("priority")),
        "payee": _drop_none({"type": _coding("http://terminology.hl7.org/CodeSystem/payeetype", c.get("payee_type")),
                             "party": {"reference": c["payee_party"]} if c.get("payee_party") else None}) or None,
        "facility": {"display": c["facility"]} if c.get("facility") else None,
        "prescription": {"identifier": {"value": c["prescription_identifier"]}} if c.get("prescription_identifier") else None,
        "referral": {"reference": c["referral"]} if c.get("referral") else None,
        "accident": accident or None,
        "related": [_drop_none({
            "claim": {"identifier": {"value": r["claim_identifier"]}} if r.get("claim_identifier") else None,
            "relationship": _coding(f"{_TERMINOLOGY}/claim-relationship", r.get("relationship"))})
            for r in tables.get("claim_related") or []] or None,
        "careTeam": care_team or None,
        "supportingInfo": supporting_info or None,
        "diagnosis": diagnosis or None,
        "insurance": insurance or None,
        "item": items or None,
        "total": money(c.get("total")),
    })
    add(claim, claim_url)

    return {
        "resourceType": "Bundle", "id": c.get("bundle_id") or str(uuid.uuid4()),
        "meta": {"profile": [f"{_DHAMANI}/bundle|1.0.0"]},
        "type": "message",
        "timestamp": datetime.now(timezone(timedelta(hours=4))).isoformat(timespec="milliseconds"),
        "entry": entries,
    }
