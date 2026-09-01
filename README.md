# JSON2FHIR - FastAPI Microservices Architecture

A complete microservices-based system for converting JSON patient data to FHIR format and integrating with hospital systems via Kafka message streaming.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         YOUR SYSTEM                                   │
│                    (JSON Input/Output)                                │
└────────────────┬────────────────────────────────┬───────────────────┘
                 │                                │
                 │ JSON                           │ JSON
                 ▼                                │
         ┌──────────────────┐                    │
         │ Integration API  │                    │
         │  (FastAPI 8000)  │                    │
         └────────┬─────────┘                    │
                  │                              │
                  │ json.request                 │
                  │ (Kafka)                      │
                  ▼                              │
         ┌──────────────────────────┐            │
         │ JSON→FHIR Service        │            │
         │ (Python Consumer)        │            │
         └────────┬─────────────────┘            │
                  │                              │
                  │ fhir.outgoing                │
                  │ (Kafka)                      │
                  ▼                              │
         ┌──────────────────────────┐            │
         │  FHIR Validation         │            │
         │  & Processing            │            │
         └────────┬─────────────────┘            │
                  │                              │
                  │                              │
                  ▼                              │
         ┌──────────────────────────┐            │
         │   MySQL Database         │            │
         │  (Transactions, Requests)│            │
         └──────────────────────────┘            │
                  │                              │
                  ▼                              │
         ┌──────────────────────────┐            │
         │    Dhamani/Hospital      │            │
         │       System             │            │
         └────────┬─────────────────┘            │
                  │ FHIR Response                │
                  │                              │
                  ▼                              │
    ┌────────────────────────────────┐           │
    │ Communication Service          │           │
    │ (Receives Hospital Response)   │           │
    └─────────┬──────────────────────┘           │
              │ fhir.incoming (Kafka)            │
              ▼                                  │
    ┌────────────────────────────────┐           │
    │ FHIR→JSON Service              │           │
    │ (Transform back to JSON)       │           │
    └─────────┬──────────────────────┘           │
              │ json.response (Kafka)            │
              ▼                                  │
    ┌────────────────────────────────┐           │
    │ Processing Service             │           │
    │ (Final Database Updates)       │           │
    └────────────────────────────────┘           │
              │                                  │
              └──────────────────────────────────┘
```

## Services

### 1. Integration API (Port 8000)
- **Purpose**: Entry point for JSON patient data from external systems
- **Technology**: FastAPI + uvicorn
- **Database**: MySQL
- **Kafka Topics**: Publishes to `json.request`
- **Endpoints**:
  - `POST /patient` - Submit patient data
  - `GET /transaction/{transaction_id}` - Get transaction status
  - `GET /health` - Health check

### 2. JSON-FHIR Service
- **Purpose**: Transform JSON to FHIR Patient resource
- **Technology**: Python + aiokafka
- **Database**: MySQL
- **Kafka Topics**: 
  - Consumes from: `json.request`
  - Publishes to: `fhir.outgoing`
- **Features**:
  - JSON validation
  - FHIR transformation
  - FHIR validation
  - Database transaction logging

### 3. FHIR-JSON Service
- **Purpose**: Transform FHIR responses back to JSON
- **Technology**: Python + aiokafka
- **Database**: MySQL
- **Kafka Topics**:
  - Consumes from: `fhir.incoming`
  - Publishes to: `json.response`
- **Features**:
  - FHIR response parsing
  - JSON transformation
  - Response mapping storage

### 4. Processing Service
- **Purpose**: Final consumer that updates database with results
- **Technology**: Python + aiokafka
- **Database**: MySQL
- **Kafka Topics**:
  - Consumes from: `json.response`
- **Features**:
  - Final status updates
  - Response mapping completion
  - Audit logging

### 5. Communication Service (Port 8001)
- **Purpose**: Receives FHIR responses from hospital/Dhamani systems
- **Technology**: FastAPI + uvicorn
- **Database**: MySQL
- **Kafka Topics**: Publishes to `fhir.incoming`
- **Endpoints**:
  - `POST /fhir/response` - Receive FHIR response from hospital
  - `GET /health` - Health check

## Database Schema

### Tables

#### `transactions`
- `id`: Auto-increment primary key
- `transaction_id`: Unique transaction identifier
- `patient_id`: Patient ID
- `patient_name`: Patient name
- `status`: PENDING, PROCESSING, SUCCESS, FAILED
- `json_payload`: Original JSON data
- `fhir_payload`: Generated FHIR data
- `created_at`, `updated_at`: Timestamps

#### `fhir_requests`
- Tracks FHIR Patient resource creation
- Stores validation status and errors
- Records hospital transmission

#### `fhir_responses`
- Stores hospital/Dhamani responses
- Tracks response codes and messages
- Marks processing status

#### `response_mappings`
- Maps original JSON to final JSON response
- Tracks transformation status

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for local development)

### Running with Docker Compose

```bash
# Navigate to project directory
cd JSON2FHIR

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

**Services will be available at:**
- Integration API: `http://localhost:8000`
- Communication Service: `http://localhost:8001`
- Kafka UI: `http://localhost:8080`
- MySQL: `localhost:3306`

### Local Development Setup

```bash
# Create Python virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install shared dependencies
cd shared
pip install -r requirements.txt

# For each service, install dependencies
cd ../integration-api
pip install -r requirements.txt

# Run integration API
python -m uvicorn src.main:app --reload

# In another terminal, run each service
cd ../json-fhir-service
python src/main.py
```

## API Usage

### 1. Submit Patient Data

```bash
curl -X POST http://localhost:8000/patient \
  -H "Content-Type: application/json" \
  -d '{
    "patient_id": "P12345",
    "name": "John Doe",
    "given_name": "John",
    "family_name": "Doe",
    "phone": "+1-555-0123",
    "email": "john.doe@example.com",
    "address": "123 Main St, Anytown, USA",
    "status": "new_admission"
  }'
```

**Response:**
```json
{
  "status": "ACCEPTED",
  "message": "Patient data published to Kafka successfully",
  "transaction_id": "TXN-A1B2C3D4E5F6",
  "data": {
    "patient_id": "P12345",
    "name": "John Doe",
    "status": "new_admission"
  }
}
```

### 2. Check Transaction Status

```bash
curl http://localhost:8000/transaction/TXN-A1B2C3D4E5F6
```

**Response:**
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

### 3. Submit FHIR Response (Hospital/Dhamani)

```bash
curl -X POST http://localhost:8001/fhir/response \
  -H "Content-Type: application/json" \
  -d '{
    "original_id": "TXN-A1B2C3D4E5F6",
    "hospital_system_id": "HSP-5678",
    "status": "201 Created",
    "timestamp": "2024-01-15T10:33:00",
    "fhir_response": {
      "resourceType": "Patient",
      "id": "HSP-5678",
      "name": [
        {
          "use": "official",
          "text": "John Doe"
        }
      ]
    }
  }'
```

## Kafka Topics

| Topic | Direction | Description |
|-------|-----------|-------------|
| `json.request` | → | Initial JSON patient data |
| `fhir.outgoing` | → | Transformed FHIR Patient resource |
| `fhir.incoming` | ← | FHIR response from hospital |
| `json.response` | ← | Final JSON response to client |

## Monitoring

### Kafka UI
Access at `http://localhost:8080` to monitor:
- Topic messages
- Consumer groups
- Broker status
- Partition information

### Database
Connect to MySQL at `localhost:3306`:
- Username: `fhir_user`
- Password: `fhir_password`
- Database: `fhir_db`

```bash
mysql -h localhost -u fhir_user -p fhir_db
```

### Logs
```bash
# View all service logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f integration-api
docker-compose logs -f json-fhir-service
docker-compose logs -f processing-service
```

## Data Flow Example

```
1. Client sends JSON patient data to Integration API
   POST /patient → {"patient_id": "P123", "name": "John Doe", ...}

2. Integration API:
   - Creates transaction record in database
   - Publishes to 'json.request' Kafka topic
   - Returns transaction ID to client

3. JSON-FHIR Service (Consumer):
   - Reads from 'json.request'
   - Transforms JSON to FHIR Patient resource
   - Validates FHIR structure
   - Stores in database
   - Publishes to 'fhir.outgoing'

4. Hospital/Dhamani System:
   - Receives FHIR Patient resource
   - Processes and creates patient record
   - Generates FHIR response

5. Communication Service (API):
   - Receives FHIR response from hospital
   - Publishes to 'fhir.incoming' Kafka topic

6. FHIR-JSON Service (Consumer):
   - Reads from 'fhir.incoming'
   - Transforms FHIR response back to JSON
   - Stores response mapping
   - Publishes to 'json.response'

7. Processing Service (Consumer):
   - Reads from 'json.response'
   - Updates transaction status to SUCCESS/FAILED
   - Logs completion to console and database

8. Client can check transaction status
   GET /transaction/TXN-XXXXX → {"status": "SUCCESS", ...}
```

## Environment Variables

Create a `.env` file (optional, defaults are configured):

```env
# Database
DATABASE_URL=mysql+pymysql://fhir_user:fhir_password@localhost:3306/fhir_db

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Services
INTEGRATION_API_PORT=8000
COMMUNICATION_SERVICE_PORT=8001
```

## Common Issues & Troubleshooting

### Issue: "Failed to connect to MySQL"
**Solution**: Ensure MySQL container is running and healthy
```bash
docker-compose ps
docker-compose logs mysql
```

### Issue: "Kafka connection refused"
**Solution**: Wait for Kafka to start (health check takes ~30 seconds)
```bash
# Wait for Kafka to be healthy
docker-compose logs kafka | grep -i "broker"
```

### Issue: "ModuleNotFoundError: No module named 'shared'"
**Solution**: Ensure `sys.path` includes the shared module directory
```python
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))
```

### Issue: Database tables not created
**Solution**: Manually initialize tables
```bash
docker-compose exec mysql mysql -u fhir_user -pfhir_password fhir_db < init-db.sql
```

## Development

### Adding New Services

1. Create service directory: `mkdir my-service`
2. Create `src/main.py` with your service logic
3. Create `requirements.txt` with dependencies
4. Create `Dockerfile`
5. Add service to `docker-compose.yml`

### Extending FHIR Transformation

Edit `shared/fhir_utils.py` to customize JSON-to-FHIR mapping:
```python
def json_to_fhir_patient(patient_data):
    # Customize transformation logic here
    pass
```

## Security Considerations

- Change default MySQL password: `root_password`
- Change default MySQL user password: `fhir_password`
- Use environment variables for credentials
- Implement JWT authentication for APIs
- Add network policies for Docker containers
- Enable SSL/TLS for Kafka

## Performance Optimization

- Kafka consumer batch processing
- Database connection pooling
- Async processing with aiokafka
- Message compression in Kafka
- Database indexing on frequently queried columns

## Deployment

### Docker Compose (Development/Testing)
```bash
docker-compose up -d
```

### Kubernetes (Production)
See `k8s/` directory for Kubernetes manifests (create if needed)

### Cloud Platforms
- AWS ECS/EKS
- Google Cloud Run/GKE
- Azure Container Instances/AKS

## Contributing

1. Create feature branch
2. Make changes
3. Test locally
4. Submit pull request

## License

MIT License

## Support

For issues and questions, please check:
1. Logs: `docker-compose logs`
2. Kafka UI: `http://localhost:8080`
3. Database: `mysql` container
