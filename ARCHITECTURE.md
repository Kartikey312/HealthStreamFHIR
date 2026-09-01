# JSON2FHIR - Architecture Documentation

## System Overview

JSON2FHIR is a microservices-based system that converts JSON patient data to FHIR (Fast Healthcare Interoperability Resources) format and handles bidirectional integration with hospital systems via asynchronous Kafka message streaming.

## Design Principles

1. **Asynchronous Processing**: All data transformations happen through Kafka topics, enabling high throughput and fault tolerance
2. **Microservices Architecture**: Each service has a single responsibility and communicates via well-defined interfaces
3. **Database-Backed Reliability**: All transactions are recorded in MySQL for audit trails and recovery
4. **FHIR Compliance**: Transforms follow FHIR Patient resource standards
5. **Scalability**: Services can be scaled independently based on demand

## Service Architecture

### Service Topology

```
┌─────────────────────────────────────────────────────────────────┐
│ External System (JSON)                                           │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │   Integration API           │ (FastAPI)
        │   - HTTP Entry Point        │
        │   - Transaction Management  │
        │   - Kafka Producer          │
        └──────────┬──────────────────┘
                   │ json.request
                   ▼
        ┌────────────────────────────────┐
        │  JSON→FHIR Service             │
        │  - JSON Validation             │
        │  - FHIR Transformation         │
        │  - Kafka Consumer/Producer     │
        │  - Database Storage            │
        └──────────┬─────────────────────┘
                   │ fhir.outgoing
                   ▼
        ┌────────────────────────────────┐
        │   Hospital/Dhamani System      │
        │   (External FHIR Processor)    │
        └──────────┬─────────────────────┘
                   │ FHIR Response
                   ▼
        ┌────────────────────────────────┐
        │  Communication Service         │ (FastAPI)
        │  - HTTP Endpoint               │
        │  - Response Reception          │
        │  - Kafka Producer              │
        └──────────┬─────────────────────┘
                   │ fhir.incoming
                   ▼
        ┌────────────────────────────────┐
        │  FHIR→JSON Service             │
        │  - FHIR Response Parsing       │
        │  - JSON Transformation         │
        │  - Kafka Consumer/Producer     │
        │  - Database Storage            │
        └──────────┬─────────────────────┘
                   │ json.response
                   ▼
        ┌────────────────────────────────┐
        │  Processing Service            │
        │  - Final Results Processing    │
        │  - Database Updates            │
        │  - Kafka Consumer              │
        │  - Audit Logging               │
        └──────────┬─────────────────────┘
                   │
                   ▼
        ┌────────────────────────────────┐
        │  MySQL Database                │
        │  (Persistence Layer)           │
        └────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│ External System (Result/Confirmation)                            │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow in Detail

### 1. JSON Submission Phase

**Actors**: Client System → Integration API

```
Client System
    │
    │ POST /patient
    │ JSON: {"patient_id": "P123", "name": "John Doe", ...}
    ▼
Integration API (Port 8000)
    │
    ├─ Create Transaction Record
    │  └─ INSERT INTO transactions
    │
    ├─ Publish to Kafka
    │  └─ TOPIC: json.request
    │
    └─ Return Transaction ID
       └─ {"transaction_id": "TXN-ABC123", "status": "ACCEPTED"}
```

**Database Changes**:
- `transactions` table: NEW record with status="PROCESSING"

### 2. JSON-to-FHIR Transformation Phase

**Actors**: JSON-FHIR Service (Kafka Consumer)

```
Kafka Topic: json.request
    │
    │ Message consumed by json-fhir-service
    ▼
JSON→FHIR Service
    │
    ├─ Parse incoming JSON
    │  └─ Extract: patient_id, name, phone, address, etc.
    │
    ├─ Transform to FHIR Patient Resource
    │  └─ Create FHIR structure with:
    │     - resourceType: "Patient"
    │     - id: (patient ID)
    │     - name: [FHIR name array]
    │     - active: boolean
    │     - telecom: [phone, email]
    │     - address: []
    │
    ├─ Validate FHIR Structure
    │  ├─ Check required fields
    │  ├─ Validate data types
    │  └─ Verify FHIR compliance
    │
    ├─ Store in Database
    │  ├─ UPDATE transactions SET fhir_payload=...
    │  └─ INSERT INTO fhir_requests (...)
    │
    └─ Publish to Kafka
       └─ TOPIC: fhir.outgoing
          └─ Message: {"transaction_id": "TXN-ABC123", "fhir_resource": {...}}
```

**Database Changes**:
- `transactions` table: UPDATE status, store fhir_payload
- `fhir_requests` table: NEW record with FHIR Patient resource

### 3. Hospital Processing Phase

**Actors**: Hospital/Dhamani System (External)

```
Kafka Topic: fhir.outgoing
    │
    │ Message consumed by Hospital System
    ▼
Hospital/Dhamani System
    │
    ├─ Receive FHIR Patient Resource
    │
    ├─ Process/Create Patient Record
    │
    ├─ Validate Patient Data
    │
    ├─ Store in Hospital Database
    │
    └─ Generate FHIR Response
       └─ Status: "201 Created" or error code
       └─ Response includes: Patient ID, Hospital ID, Timestamps
```

**No Database Changes** (Hospital side)

### 4. Response Reception Phase

**Actors**: Hospital System → Communication Service

```
Hospital/Dhamani System
    │
    │ POST /fhir/response (via HTTP)
    │ Body: {"original_id": "TXN-ABC123", "hospital_system_id": "HSP-001", ...}
    ▼
Communication Service (Port 8001)
    │
    └─ Publish to Kafka
       └─ TOPIC: fhir.incoming
          └─ Message: {"transaction_id": "TXN-ABC123", "fhir_response": {...}}
```

**Database Changes**: None at Communication Service (just Kafka publishing)

### 5. FHIR-to-JSON Transformation Phase

**Actors**: FHIR-JSON Service (Kafka Consumer)

```
Kafka Topic: fhir.incoming
    │
    │ Message consumed by fhir-json-service
    ▼
FHIR→JSON Service
    │
    ├─ Parse FHIR Response
    │  └─ Extract: id, status, codes, messages
    │
    ├─ Transform back to JSON
    │  └─ Create JSON response:
    │     {
    │       "internal_patient_id": "P123",
    │       "external_reference_id": "HSP-001",
    │       "sync_status": "SUCCESS",
    │       "completed_at": "2024-01-15T10:33:00"
    │     }
    │
    ├─ Store in Database
    │  ├─ INSERT INTO fhir_responses (...)
    │  └─ INSERT INTO response_mappings (...)
    │
    └─ Publish to Kafka
       └─ TOPIC: json.response
          └─ Message: {"transaction_id": "TXN-ABC123", "sync_status": "SUCCESS", ...}
```

**Database Changes**:
- `fhir_responses` table: NEW record with hospital response
- `response_mappings` table: NEW record mapping original JSON to final JSON

### 6. Final Processing Phase

**Actors**: Processing Service (Kafka Consumer)

```
Kafka Topic: json.response
    │
    │ Message consumed by processing-service
    ▼
Processing Service
    │
    ├─ Parse final JSON response
    │
    ├─ Update Transaction Status
    │  └─ UPDATE transactions SET status='SUCCESS' ...
    │
    ├─ Update Response Mapping
    │  └─ UPDATE response_mappings SET status='COMPLETED' ...
    │
    └─ Log Completion
       └─ Console output with transaction details
```

**Database Changes**:
- `transactions` table: UPDATE status to SUCCESS/FAILED
- `response_mappings` table: UPDATE status to COMPLETED

## Kafka Topics and Message Formats

### Topic: `json.request`
**Producer**: Integration API
**Consumer**: JSON-FHIR Service
**Message Format**:
```json
{
  "transaction_id": "TXN-A1B2C3D4E5F6",
  "patient_id": "P12345",
  "patient_name": "John Doe",
  "status": "new_admission",
  "payload": {
    "patient_id": "P12345",
    "name": "John Doe",
    "given_name": "John",
    "family_name": "Doe",
    "phone": "+1-555-0123",
    "email": "john.doe@example.com",
    "address": "123 Main St, Anytown, USA",
    "status": "new_admission"
  }
}
```

### Topic: `fhir.outgoing`
**Producer**: JSON-FHIR Service
**Consumer**: Hospital/Dhamani System
**Message Format**:
```json
{
  "transaction_id": "TXN-A1B2C3D4E5F6",
  "patient_id": "P12345",
  "patient_name": "John Doe",
  "fhir_resource": {
    "resourceType": "Patient",
    "id": "P12345",
    "name": [
      {
        "use": "official",
        "text": "John Doe",
        "given": ["John"],
        "family": "Doe"
      }
    ],
    "active": true,
    "telecom": [
      {"system": "phone", "value": "+1-555-0123"},
      {"system": "email", "value": "john.doe@example.com"}
    ],
    "address": [
      {"use": "home", "text": "123 Main St, Anytown, USA"}
    ],
    "meta": {
      "lastUpdated": "2024-01-15T10:30:00Z",
      "source": "json2fhir"
    }
  }
}
```

### Topic: `fhir.incoming`
**Producer**: Communication Service
**Consumer**: FHIR-JSON Service
**Message Format**:
```json
{
  "transaction_id": "TXN-A1B2C3D4E5F6",
  "patient_id": "P12345",
  "fhir_response": {
    "id": "HSP-5678",
    "code": 201,
    "message": "201 Created",
    "status": "201 Created",
    "timestamp": "2024-01-15T10:33:00Z",
    "originalId": "TXN-A1B2C3D4E5F6",
    "hospitalSystemId": "HSP-5678",
    "fhir_resource": {
      "resourceType": "Patient",
      "id": "HSP-5678",
      "name": [{"use": "official", "text": "John Doe"}]
    }
  }
}
```

### Topic: `json.response`
**Producer**: FHIR-JSON Service
**Consumer**: Processing Service
**Message Format**:
```json
{
  "transaction_id": "TXN-A1B2C3D4E5F6",
  "internal_patient_id": "P12345",
  "external_reference_id": "HSP-5678",
  "sync_status": "SUCCESS",
  "hospital_response_code": 201,
  "hospital_system_id": "HSP-5678",
  "completed_at": "2024-01-15T10:33:05Z",
  "fhir_response": { ... }
}
```

## Database Schema Relationships

```
transactions (Primary Record)
    │
    ├─→ fhir_requests (1:Many)
    │   └─ Stores FHIR Patient resources created
    │
    ├─→ fhir_responses (1:Many)
    │   └─ Stores Hospital responses received
    │
    └─→ response_mappings (1:1)
        └─ Maps JSON input to JSON output
```

## Error Handling and Recovery

### Validation Failures
1. **JSON Validation**: Invalid JSON payload → HTTP 400 returned
2. **FHIR Validation**: Invalid FHIR structure → Transaction marked FAILED
3. **Database Errors**: Connection failures → Kafka consumer retries with exponential backoff

### Recovery Mechanisms
- **Message Idempotency**: Transaction IDs ensure duplicate messages are ignored
- **Transaction Logging**: All states saved to database for recovery
- **Dead Letter Queues**: Failed messages can be replayed
- **Health Checks**: Periodic service availability verification

## Scalability Considerations

### Horizontal Scaling
- **Kafka Consumer Groups**: Multiple instances of JSON-FHIR service can consume from `json.request`
- **Database Pooling**: Connection pooling handles increased concurrent connections
- **Load Balancing**: Multiple Integration API instances behind load balancer

### Performance Tuning
- **Batch Processing**: Kafka consumers can process messages in batches
- **Database Indexing**: Indexes on transaction_id, patient_id, status, timestamps
- **Caching Layer**: Redis can cache frequently accessed transaction data
- **Async Processing**: All I/O operations are async (aiokafka, async SQLAlchemy)

## Security Architecture

### Data Protection
- **Database**: MySQL with user authentication and password protection
- **Kafka**: Can be configured with SSL/TLS and SASL authentication
- **APIs**: Can implement JWT token validation
- **Network**: Docker network isolation between services

### Access Control
- **Database Access**: Limited to fhir_user with specific database permissions
- **API Authentication**: Should implement OAuth 2.0 or API keys
- **Service-to-Service**: Network policies restrict inter-service communication

### Audit Trail
- All transactions logged with timestamps and user information
- Database change tracking via updated_at timestamps
- Kafka message retention for debugging

## Monitoring and Logging

### Log Aggregation Points
1. **Integration API**: HTTP requests/responses
2. **Kafka**: Message production/consumption
3. **Services**: Transformation logs and errors
4. **Database**: Transaction logs and changes

### Metrics to Monitor
- Message throughput (messages/second)
- End-to-end latency (submission to completion)
- Error rate and failure types
- Database connection pool utilization
- Kafka consumer lag

### Alerting
- Service health check failures
- High error rates (>1% of messages)
- Consumer lag exceeding threshold (>1000 messages)
- Database connection pool exhaustion
- Disk space warnings

## Deployment Architecture

### Development
- Docker Compose with all services in one network
- Shared database for all services
- Kafka in single-broker mode

### Production (Kubernetes)
```yaml
Namespaces:
  - integration-api
  - processing-services
  - database

Deployments:
  - integration-api (3 replicas)
  - json-fhir-service (2 replicas)
  - fhir-json-service (2 replicas)
  - processing-service (1 replica)
  - communication-service (2 replicas)

StatefulSets:
  - kafka (3 brokers)
  - mysql (1 primary + 1 replica)

ConfigMaps:
  - Database connection strings
  - Kafka broker addresses
  - Service configurations

Secrets:
  - Database passwords
  - API keys
  - TLS certificates
```

### Cloud Deployment
- **AWS**: ECS/EKS with RDS MySQL, MSK Kafka
- **GCP**: Cloud Run + Cloud SQL + Pub/Sub
- **Azure**: AKS + Azure Database for MySQL + Event Hubs

## Disaster Recovery

### Backup Strategy
- **Database**: Automated daily backups with 30-day retention
- **Kafka**: Message retention policy (7 days minimum)
- **Configuration**: Version controlled in Git

### Recovery Procedures
1. **Service Restart**: Kubernetes automatically restarts failed pods
2. **Database Restore**: Point-in-time recovery from backups
3. **Message Replay**: Failed messages can be reprocessed from Kafka offset

### RTO/RPO Targets
- **RTO** (Recovery Time Objective): < 5 minutes
- **RPO** (Recovery Point Objective): < 1 minute

## Cost Optimization

### Resource Allocation
- **CPU**: 0.5 core per service (can be reduced during off-hours)
- **Memory**: 256MB per service
- **Storage**: MySQL 20GB + Kafka 50GB (auto-scaling enabled)

### Cost Reduction Strategies
- Use spot instances for non-critical services
- Implement auto-scaling based on queue depth
- Archive old transaction data to cold storage
- Optimize Kafka retention policies
