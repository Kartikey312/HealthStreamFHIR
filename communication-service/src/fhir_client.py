"""
Outbound FHIR client per implementation.md section 6.4 - calls the external
FHIR server (HAPI FHIR locally, per section 8) with retries. Conditional PUT
on identifier makes retries idempotent (no duplicate Patients on redelivery).
"""
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type


class FhirClient:
    def __init__(self, base_url: str):
        self._c = httpx.AsyncClient(
            base_url=base_url, timeout=15,
            headers={"Content-Type": "application/fhir+json"},
        )

    async def close(self):
        await self._c.aclose()

    @retry(stop=stop_after_attempt(4), wait=wait_exponential_jitter(1, 10),
           retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)))
    async def upsert_patient(self, patient: dict) -> httpx.Response:
        ident = patient["identifier"][0]
        r = await self._c.put(
            f"/Patient?identifier={ident['system']}|{ident['value']}",
            json=patient,
        )
        if r.status_code >= 500 or r.status_code == 429:
            r.raise_for_status()  # retryable
        return r
