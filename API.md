# API Documentation

## Base URLs
- **Integration API**: `http://localhost:8000`
- **Communication Service**: `http://localhost:8001`

## Authentication
Currently, APIs are open. For production, add:
- JWT Bearer tokens
- API keys in headers
- OAuth 2.0 flow

## Integration API Endpoints

### 1. Health Check
**Endpoint**: `GET /health`
**Description**: Check if service is running
**Response**:
```json
{
  "status": "healthy",
  "service": "Integration API",
  "version": "1.0.0"
}
```
**HTTP Status**: 200 OK

---

### 2. Submit Patient Data
**Endpoint**: `POST /patient`
**Description**: Submit JSON patient data for FHIR conversion

**Request Headers**:
```
Content-Type: application/json
```

**Request Body**:
```json
{
  "patient_id": "P12345",
  "name": "John Doe",
  "given_name": "John",
  "family_name": "Doe",
  "phone": "+1-555-0123",
  "email": "john.doe@example.com",
  "address": "123 Main Street, Springfield, USA",
  "status": "new_admission"
}
```

**Required Fields**:
- `patient_id` (string): Unique patient identifier
- `name` (string): Full name of patient

**Optional Fields**:
- `given_name` (string): First name
- `family_name` (string): Last name
- `phone` (string): Phone number
- `email` (string): Email address
- `address` (string): Physical address
- `status` (string): Patient status (default: "new_admission")

**Response**:
```json
{
  "status": "ACCEPTED",
  "message": "Patient data published to Kafka successfully",
  "transaction_id": "TXN-A1B2C3D4E5F6",
  "data": {
    "patient_id": "P12345",
    "name": "John Doe",
    "given_name": "John",
    "family_name": "Doe",
    "phone": "+1-555-0123",
    "email": "john.doe@example.com",
    "address": "123 Main Street, Springfield, USA",
    "status": "new_admission"
  }
}
```

**Response Fields**:
- `status` (string): "ACCEPTED" - Request accepted
- `message` (string): Human-readable message
- `transaction_id` (string): Unique identifier for tracking
- `data` (object): Echo of submitted data

**HTTP Status Codes**:
- `202 Accepted`: Successfully received and queued
- `400 Bad Request`: Missing required fields or invalid data
- `500 Internal Server Error`: Server error during processing

**cURL Example**:
```bash
curl -X POST http://localhost:8000/patient \
  -H "Content-Type: application/json" \
  -d '{
    "patient_id": "P12345",
    "name": "John Doe",
    "phone": "+1-555-0123",
    "status": "new_admission"
  }'
```

**Error Responses**:

*Missing Required Field*:
```json
{
  "detail": "Request body cannot be empty"
}
```
Status: 400

*Server Error*:
```json
{
  "detail": "Failed to publish message to Kafka"
}
```
Status: 500

---

### 3. Get Transaction Status
**Endpoint**: `GET /transaction/{transaction_id}`
**Description**: Retrieve current status of a transaction

**Path Parameters**:
- `transaction_id` (string): Transaction ID from submission response

**Response**:
```json
{
  "transaction_id": "TXN-A1B2C3D4E5F6",
  "status": "SUCCESS",
  "patient_id": "P12345",
  "patient_name": "John Doe",
  "created_at": "2024-01-15T10:30:00",
  "updated_at": "2024-01-15T10:35:45"
}
```

**Response Fields**:
- `transaction_id` (string): Transaction identifier
- `status` (string): Current status
  - `PENDING`: Awaiting processing
  - `PROCESSING`: Currently being processed
  - `SUCCESS`: Completed successfully
  - `FAILED`: Processing failed
- `patient_id` (string): Patient identifier
- `patient_name` (string): Patient name
- `created_at` (datetime): Transaction creation time
- `updated_at` (datetime): Last update time

**HTTP Status Codes**:
- `200 OK`: Transaction found
- `404 Not Found`: Transaction ID does not exist
- `500 Internal Server Error`: Server error

**cURL Example**:
```bash
curl http://localhost:8000/transaction/TXN-A1B2C3D4E5F6
```

---

## Communication Service Endpoints

### 1. Health Check
**Endpoint**: `GET /health`
**Description**: Check if service is running
**Response**:
```json
{
  "status": "healthy",
  "service": "Communication Service",
  "version": "1.0.0"
}
```
**HTTP Status**: 200 OK

---

### 2. Receive FHIR Response
**Endpoint**: `POST /fhir/response`
**Description**: Receive FHIR response from hospital/Dhamani system

**Request Headers**:
```
Content-Type: application/json
```

**Request Body**:
```json
{
  "original_id": "TXN-A1B2C3D4E5F6",
  "hospital_system_id": "HSP-2024-001",
  "status": "201 Created",
  "timestamp": "2024-01-15T10:33:00Z",
  "fhir_response": {
    "resourceType": "Patient",
    "id": "HSP-2024-001",
    "name": [
      {
        "use": "official",
        "text": "John Doe"
      }
    ],
    "active": true,
    "telecom": [
      {
        "system": "phone",
        "value": "+1-555-0123"
      }
    ]
  }
}
```

**Required Fields**:
- `original_id` (string): Original transaction ID
- `hospital_system_id` (string): Hospital's patient ID
- `status` (string): HTTP status message or "201 Created"
- `timestamp` (datetime): ISO 8601 format
- `fhir_response` (object): FHIR Patient resource

**Response**:
```json
{
  "status": "received",
  "message": "FHIR response received and published",
  "response_id": "RESP-A1B2C3D4E5F6",
  "transaction_id": "TXN-A1B2C3D4E5F6"
}
```

**HTTP Status Codes**:
- `200 OK`: Response successfully received
- `400 Bad Request`: Missing required fields
- `500 Internal Server Error`: Server error

**cURL Example**:
```bash
curl -X POST http://localhost:8001/fhir/response \
  -H "Content-Type: application/json" \
  -d '{
    "original_id": "TXN-A1B2C3D4E5F6",
    "hospital_system_id": "HSP-2024-001",
    "status": "201 Created",
    "timestamp": "2024-01-15T10:33:00Z",
    "fhir_response": {
      "resourceType": "Patient",
      "id": "HSP-2024-001",
      "name": [{"use": "official", "text": "John Doe"}]
    }
  }'
```

---

## Response Status Codes

| Code | Meaning | When Used |
|------|---------|-----------|
| 200 | OK | Successful GET request |
| 202 | Accepted | Request queued for processing |
| 400 | Bad Request | Invalid input parameters |
| 404 | Not Found | Resource (transaction) not found |
| 500 | Internal Server Error | Server-side error |

---

## Common Error Codes and Solutions

### Error: "Request body cannot be empty"
**Cause**: Missing required fields in JSON body
**Solution**: Include `patient_id` and `name` fields
```json
{
  "patient_id": "P12345",
  "name": "John Doe"
}
```

### Error: "Failed to publish message to Kafka"
**Cause**: Kafka broker not accessible
**Solution**: Check if Kafka container is running
```bash
docker-compose logs kafka
docker-compose ps kafka
```

### Error: "Transaction not found"
**Cause**: Invalid transaction ID
**Solution**: Verify transaction ID is correct and use exact ID from submission response

---

## Rate Limiting
Currently not implemented. For production, recommend:
- 1000 requests/minute per API key
- 100 concurrent connections per client

---

## Pagination
Not required for current API design. For future enhancement:
- Support `?page=1&limit=10` for list endpoints
- Include `total_count`, `page`, `limit` in responses

---

## WebSocket Support
Currently not implemented. Can be added for real-time transaction status updates:
- `ws://localhost:8000/ws/transaction/{transaction_id}`

---

## API Versioning
Current version: v1 (implicit)
Future versioning strategy:
- URL-based: `/api/v2/patient`
- Header-based: `Accept: application/vnd.api+json;version=2`

---

## OpenAPI/Swagger Documentation
Available at:
- Integration API: `http://localhost:8000/docs`
- Integration API (ReDoc): `http://localhost:8000/redoc`
- Communication Service: `http://localhost:8001/docs`
- Communication Service (ReDoc): `http://localhost:8001/redoc`

---

## Example Workflow

### Complete Flow with Timestamps

```
T+0s   | Client POSTs to /patient
T+0s   | Integration API returns transaction_id = TXN-ABC123
T+1s   | JSON message in kafka topic: json.request
T+2s   | JSON-FHIR service consumes message
T+2s   | FHIR Patient resource created and validated
T+3s   | FHIR message in kafka topic: fhir.outgoing
T+3s   | Hospital system processes FHIR
T+10s  | Hospital POSTs FHIR response to Communication API
T+10s  | FHIR message in kafka topic: fhir.incoming
T+11s  | FHIR-JSON service consumes message
T+11s  | JSON response created
T+12s  | JSON message in kafka topic: json.response
T+12s  | Processing service marks transaction SUCCESS
T+0s+  | Client GETs /transaction/TXN-ABC123
       | Response shows status = SUCCESS
```

---

## Best Practices

1. **Always store transaction_id**: Save returned transaction ID for status tracking
2. **Implement exponential backoff**: Retry failed requests with increasing delays
3. **Handle async nature**: Don't expect immediate results; poll status periodically
4. **Validate patient data**: Ensure data quality before submission
5. **Monitor logs**: Watch service logs for any errors or warnings

---

## Support

For API issues:
1. Check service health: `GET /health`
2. Review logs: `docker-compose logs -f`
3. Verify Kafka connectivity: `http://localhost:8080` (Kafka UI)
4. Check database: `mysql -u fhir_user -p fhir_db`
