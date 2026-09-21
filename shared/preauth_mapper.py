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
