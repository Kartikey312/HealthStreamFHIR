# Quick Reference Guide

## Essential Commands

### Start/Stop Services

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart a specific service
docker-compose restart integration-api

# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f integration-api
```

### Testing

```bash
# Run full end-to-end test (Linux/macOS)
bash test-flow.sh

# Run full end-to-end test (Windows PowerShell)
.\test-flow.ps1

# Test individual endpoint
curl http://localhost:8000/health
```

### Database Operations

```bash
# Connect to MySQL
docker-compose exec mysql mysql -u fhir_user -pfhir_password fhir_db

# View transactions
SELECT * FROM transactions;

# View FHIR requests
SELECT * FROM fhir_requests;

# View response mappings
SELECT * FROM response_mappings;

# Backup database
docker-compose exec mysql mysqldump -u fhir_user -pfhir_password fhir_db > backup.sql

# Check database size
SELECT table_schema, ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) FROM information_schema.tables GROUP BY table_schema;
```

### Kafka Operations

```bash
# View Kafka UI
open http://localhost:8080

# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092

# View messages in topic
docker-compose exec kafka kafka-console-consumer \
  --topic json.request \
  --from-beginning \
  --bootstrap-server kafka:9092

# Check consumer group lag
docker-compose exec kafka kafka-consumer-groups \
  --group json-fhir-group \
  --describe \
  --bootstrap-server kafka:9092
```

### Local Development

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies for a service
pip install -r integration-api/requirements.txt

# Run service with reload
python -m uvicorn src.main:app --reload

# Run test script
python src/main.py
```

## Port Mapping

| Service | Port | Purpose |
|---------|------|---------|
| Integration API | 8000 | Submit patient data |
| Communication Service | 8001 | Receive hospital response |
| Kafka UI | 8080 | Kafka monitoring |
| MySQL | 3306 | Database access |
| Zookeeper | 2181 | Kafka coordination |
| Kafka Broker | 9092 | Kafka external |
| Kafka Internal | 29092 | Kafka internal (Docker) |

## Environment Variables

```bash
# Database
DATABASE_URL=mysql+pymysql://fhir_user:fhir_password@mysql:3306/fhir_db

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Service Ports
INTEGRATION_API_PORT=8000
COMMUNICATION_SERVICE_PORT=8001

# MySQL
MYSQL_ROOT_PASSWORD=root_password
MYSQL_USER=fhir_user
MYSQL_PASSWORD=fhir_password
MYSQL_DATABASE=fhir_db
```

## File Locations

```bash
# Main files
README.md                 # Start here
ARCHITECTURE.md           # Technical details
API.md                    # API reference
DEPLOYMENT.md             # Deployment guide

# Shared modules
shared/database.py        # DB configuration
shared/models.py          # ORM models
shared/schemas.py         # Validation
shared/kafka_utils.py     # Kafka helpers
shared/fhir_utils.py      # FHIR transforms

# Services
integration-api/src/main.py
json-fhir-service/src/main.py
fhir-json-service/src/main.py
processing-service/src/main.py
communication-service/src/main.py

# Configuration
docker-compose.yml        # Docker setup
init-db.sql              # DB schema
.env.example             # Config template

# Testing
test-flow.sh             # Bash test
test-flow.ps1            # PowerShell test
```

## Common Issues & Solutions

### Issue: "Connection refused" for Kafka

**Solution**:
```bash
# Check Kafka status
docker-compose logs kafka

# Restart Kafka
docker-compose restart kafka

# Wait 30 seconds for startup
sleep 30
```

### Issue: "MySQL connection error"

**Solution**:
```bash
# Check MySQL status
docker-compose logs mysql

# Restart MySQL
docker-compose restart mysql

# Verify connection
docker-compose exec mysql mysql -u fhir_user -pfhir_password fhir_db -e "SELECT 1"
```

### Issue: "ModuleNotFoundError: No module named 'shared'"

**Solution**:
```bash
# Ensure PYTHONPATH includes shared
export PYTHONPATH=/path/to/JSON2FHIR:$PYTHONPATH

# Or modify sys.path in code (already done)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))
```

### Issue: Docker image build fails

**Solution**:
```bash
# Clear Docker cache
docker-compose build --no-cache

# Check Dockerfile syntax
docker build -f integration-api/Dockerfile --no-cache .
```

### Issue: "Port already in use"

**Solution**:
```bash
# Find process using port
lsof -i :8000  # macOS/Linux
netstat -ano | findstr :8000  # Windows

# Kill process
kill -9 <PID>  # macOS/Linux
taskkill /PID <PID> /F  # Windows

# Or use different ports in docker-compose.yml
```

## Performance Tuning

### For High Throughput
```yaml
# docker-compose.yml
integration-api:
  environment:
    WORKERS: 4
  resources:
    limits:
      cpus: '2'
      memory: 1G
```

### For Multiple Instances
```bash
docker-compose up -d --scale json-fhir-service=3
```

### Database Optimization
```sql
-- Add indexes for common queries
CREATE INDEX idx_transaction_status ON transactions(status);
CREATE INDEX idx_created_at ON transactions(created_at);
```

## Monitoring Checklist

- [ ] All services running: `docker-compose ps`
- [ ] Database accessible: `docker-compose exec mysql mysql -e "SELECT 1"`
- [ ] Kafka healthy: Check http://localhost:8080
- [ ] APIs responding: `curl http://localhost:8000/health`
- [ ] No error logs: `docker-compose logs --tail=100`
- [ ] Database has data: `SELECT COUNT(*) FROM transactions`

## Development Workflow

### 1. Make Changes
```bash
# Edit service code
vim integration-api/src/main.py

# With auto-reload enabled, changes apply instantly
```

### 2. Test Changes
```bash
# Test specific endpoint
curl -X POST http://localhost:8000/patient \
  -H "Content-Type: application/json" \
  -d '{"patient_id":"P123","name":"Test"}'
```

### 3. Check Database
```bash
# View transaction records
docker-compose exec mysql mysql -u fhir_user -pfhir_password fhir_db \
  -e "SELECT * FROM transactions ORDER BY created_at DESC LIMIT 5"
```

### 4. Review Logs
```bash
# Check specific service logs
docker-compose logs -f integration-api --tail=50
```

## Debugging Tips

### Enable Debug Logging
```python
# In main.py
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
```

### Inspect Kafka Messages
```bash
# Start consumer from beginning
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic json.request \
  --from-beginning \
  --property print.timestamp=true
```

### Database Query Analysis
```bash
# Show query execution plan
EXPLAIN SELECT * FROM transactions WHERE status = 'SUCCESS';

# Check table size
SELECT table_name, ROUND(((data_length + index_length) / 1024 / 1024), 2) FROM information_schema.TABLES WHERE table_schema = 'fhir_db';
```

### Service Health Diagnostics
```bash
# Check all services
for svc in mysql kafka integration-api json-fhir-service fhir-json-service processing-service communication-service; do
  echo "=== $svc ===" 
  docker-compose ps $svc
done
```

## Useful Links

- **FastAPI Documentation**: https://fastapi.tiangolo.com/
- **Kafka Documentation**: https://kafka.apache.org/documentation/
- **SQLAlchemy ORM**: https://docs.sqlalchemy.org/
- **MySQL Documentation**: https://dev.mysql.com/doc/
- **FHIR Specification**: https://www.hl7.org/fhir/

## Version Information

```bash
# Check Python version
python --version

# Check Docker version
docker --version

# Check Docker Compose version
docker-compose --version

# Check pip packages
pip list
```

## Getting Help

1. **Check Logs**: `docker-compose logs -f`
2. **Review Documentation**: See ARCHITECTURE.md and API.md
3. **Test Endpoints**: Run test-flow.sh or test-flow.ps1
4. **Database Check**: Connect via MySQL CLI
5. **Kafka Monitor**: Visit http://localhost:8080

## Security Reminders

- [ ] Change default MySQL passwords in production
- [ ] Use environment variables for secrets
- [ ] Enable authentication for Kafka
- [ ] Use HTTPS/TLS for APIs
- [ ] Implement API authentication (JWT/API keys)
- [ ] Regular database backups
- [ ] Monitor logs for suspicious activity
- [ ] Keep dependencies updated

## Performance Targets

- API response time: < 100ms
- End-to-end latency: < 5 seconds
- Kafka message throughput: > 1000 messages/second
- Database query time: < 10ms (with indexes)
- CPU usage per service: < 50%
- Memory usage per service: < 256MB

## Next Steps

1. Start with `docker-compose up -d`
2. Run `./test-flow.sh` to verify setup
3. Check `http://localhost:8000/docs` for API
4. Review logs with `docker-compose logs -f`
5. Read ARCHITECTURE.md for details
6. Deploy to your infrastructure (see DEPLOYMENT.md)
