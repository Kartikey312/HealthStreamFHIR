# Deployment Guide

## Quick Start (Development)

### Prerequisites
- Docker & Docker Compose >= 2.0
- Git
- 8GB RAM minimum
- 2 CPU cores minimum

### Installation Steps

```bash
# 1. Clone repository
cd JSON2FHIR

# 2. Copy environment file
cp .env.example .env

# 3. Start all services
docker-compose up -d

# 4. Wait for services to be healthy (30-60 seconds)
docker-compose ps

# 5. Run test script
./test-flow.sh
```

### Verify Installation

```bash
# Check all services are running
docker-compose ps

# Check logs for any errors
docker-compose logs

# Test Integration API
curl http://localhost:8000/health

# Test Communication Service
curl http://localhost:8001/health

# Test Workflow Service
curl http://localhost:8002/health

# Open the Workflow Builder UI
open http://localhost:5173

# Access Kafka UI
open http://localhost:8080
```

---

## Local Development Setup

### Prerequisites
- Python 3.11+
- pip/poetry
- Docker Compose (for Kafka/MySQL only)

### Setup

```bash
# 1. Start only infrastructure services
docker-compose up -d mysql kafka zookeeper kafka-ui

# 2. Create virtual environment
python -m venv venv

# 3. Activate virtual environment
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate  # Windows

# 4. Install shared dependencies
cd shared
pip install -r requirements.txt
cd ..

# 5. Run services in separate terminals

# Terminal 1: Integration API
cd integration-api
pip install -r requirements.txt
python -m uvicorn src.main:app --reload

# Terminal 2: JSON-FHIR Service
cd json-fhir-service
pip install -r requirements.txt
python src/main.py

# Terminal 3: FHIR-JSON Service
cd fhir-json-service
pip install -r requirements.txt
python src/main.py

# Terminal 4: Processing Service
cd processing-service
pip install -r requirements.txt
python src/main.py

# Terminal 5: Communication Service
cd communication-service
pip install -r requirements.txt
python -m uvicorn src.main:app --reload --port 8001
```

---

## Docker Compose Deployment

### Starting Services

```bash
# Start all services (detached)
docker-compose up -d

# Start with logs
docker-compose up

# Start specific service
docker-compose up integration-api

# Scale service
docker-compose up -d --scale json-fhir-service=3
```

### Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v

# Stop specific service
docker-compose stop json-fhir-service
```

### Viewing Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f integration-api

# View last 100 lines
docker-compose logs --tail=100

# View logs with timestamps
docker-compose logs -f --timestamps
```

### Database Management

```bash
# Connect to MySQL
docker-compose exec mysql mysql -u fhir_user -pfhir_password fhir_db

# Execute SQL file
docker-compose exec mysql mysql -u fhir_user -pfhir_password fhir_db < script.sql

# Backup database
docker-compose exec mysql mysqldump -u fhir_user -pfhir_password fhir_db > backup.sql

# Restore database
docker-compose exec -T mysql mysql -u fhir_user -pfhir_password fhir_db < backup.sql
```

### Kafka Management

```bash
# Create topic
docker-compose exec kafka kafka-topics --create \
  --topic my-topic \
  --bootstrap-server kafka:9092

# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092

# View topic messages
docker-compose exec kafka kafka-console-consumer \
  --topic json.request \
  --from-beginning \
  --bootstrap-server kafka:9092

# Delete topic
docker-compose exec kafka kafka-topics --delete \
  --topic my-topic \
  --bootstrap-server kafka:9092
```

---

## Production Deployment (Kubernetes)

### Prerequisites
- Kubernetes cluster (1.24+)
- kubectl configured
- Helm 3+
- Container registry (Docker Hub, ECR, GCR, etc.)

### Build and Push Docker Images

```bash
# Build images
docker build -t myregistry/json2fhir-integration-api:1.0.0 -f integration-api/Dockerfile .
docker build -t myregistry/json2fhir-json-fhir-service:1.0.0 -f json-fhir-service/Dockerfile .
docker build -t myregistry/json2fhir-fhir-json-service:1.0.0 -f fhir-json-service/Dockerfile .
docker build -t myregistry/json2fhir-processing-service:1.0.0 -f processing-service/Dockerfile .
docker build -t myregistry/json2fhir-communication-service:1.0.0 -f communication-service/Dockerfile .

# Push to registry
docker push myregistry/json2fhir-integration-api:1.0.0
docker push myregistry/json2fhir-json-fhir-service:1.0.0
docker push myregistry/json2fhir-fhir-json-service:1.0.0
docker push myregistry/json2fhir-processing-service:1.0.0
docker push myregistry/json2fhir-communication-service:1.0.0
```

### Deploy on Kubernetes

#### 1. Create Namespace

```bash
kubectl create namespace json2fhir
kubectl config set-context --current --namespace=json2fhir
```

#### 2. Create ConfigMap and Secrets

```bash
# Create ConfigMap for application settings
kubectl create configmap app-config \
  --from-literal=ENVIRONMENT=production \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -n json2fhir

# Create Secret for database credentials
kubectl create secret generic db-secret \
  --from-literal=DB_USER=fhir_user \
  --from-literal=DB_PASSWORD=secure_password_here \
  -n json2fhir
```

#### 3. Deploy MySQL

```yaml
# mysql-deployment.yaml
apiVersion: v1
kind: Service
metadata:
  name: mysql
  namespace: json2fhir
spec:
  ports:
    - port: 3306
      targetPort: 3306
  selector:
    app: mysql
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mysql
  namespace: json2fhir
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mysql
  template:
    metadata:
      labels:
        app: mysql
    spec:
      containers:
      - name: mysql
        image: mysql:8.0
        env:
        - name: MYSQL_ROOT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: DB_PASSWORD
        - name: MYSQL_DATABASE
          value: fhir_db
        - name: MYSQL_USER
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: DB_USER
        - name: MYSQL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: DB_PASSWORD
        ports:
        - containerPort: 3306
        volumeMounts:
        - name: mysql-storage
          mountPath: /var/lib/mysql
        - name: init-script
          mountPath: /docker-entrypoint-initdb.d
      volumes:
      - name: mysql-storage
        persistentVolumeClaim:
          claimName: mysql-pvc
      - name: init-script
        configMap:
          name: init-db
```

#### 4. Deploy Kafka (using Confluent Helm Chart)

```bash
# Add Helm repository
helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts
helm repo update

# Install Kafka
helm install kafka confluentinc/cp-kafka \
  --namespace json2fhir \
  --values kafka-values.yaml
```

#### 5. Deploy Services

```yaml
# integration-api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: integration-api
  namespace: json2fhir
spec:
  replicas: 2
  selector:
    matchLabels:
      app: integration-api
  template:
    metadata:
      labels:
        app: integration-api
    spec:
      containers:
      - name: integration-api
        image: myregistry/json2fhir-integration-api:1.0.0
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          value: "mysql+pymysql://$(DB_USER):$(DB_PASSWORD)@mysql:3306/fhir_db"
        - name: KAFKA_BOOTSTRAP_SERVERS
          valueFrom:
            configMapKeyRef:
              name: app-config
              key: KAFKA_BOOTSTRAP_SERVERS
        envFrom:
        - secretRef:
            name: db-secret
        resources:
          requests:
            memory: "256Mi"
            cpu: "500m"
          limits:
            memory: "512Mi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: integration-api
  namespace: json2fhir
spec:
  type: LoadBalancer
  ports:
    - port: 80
      targetPort: 8000
  selector:
    app: integration-api
```

Apply configuration:
```bash
kubectl apply -f mysql-deployment.yaml
kubectl apply -f kafka-deployment.yaml
kubectl apply -f integration-api-deployment.yaml
# ... repeat for other services
```

#### 6. Deploy Communication Service

Similar to integration-api but on port 8001

#### 7. Deploy Worker Services (json-fhir, fhir-json, processing)

Similar deployments but without LoadBalancer service (ClusterIP only)

### Verify Deployment

```bash
# Check pod status
kubectl get pods -n json2fhir

# Check services
kubectl get svc -n json2fhir

# Check deployment status
kubectl describe deployment integration-api -n json2fhir

# View logs
kubectl logs deployment/integration-api -n json2fhir -f

# Port forward for testing
kubectl port-forward svc/integration-api 8000:80 -n json2fhir
```

---

## AWS ECS Deployment

### Prerequisites
- AWS Account with ECR access
- AWS CLI configured
- ECS Cluster running

### Steps

```bash
# 1. Create ECR repositories
aws ecr create-repository --repository-name json2fhir-integration-api
aws ecr create-repository --repository-name json2fhir-json-fhir-service
# ... etc

# 2. Push images to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789.dkr.ecr.us-east-1.amazonaws.com

docker tag json2fhir-integration-api:1.0.0 123456789.dkr.ecr.us-east-1.amazonaws.com/json2fhir-integration-api:1.0.0
docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/json2fhir-integration-api:1.0.0

# 3. Create RDS MySQL instance
aws rds create-db-instance \
  --db-instance-identifier json2fhir-mysql \
  --db-instance-class db.t3.micro \
  --engine mysql \
  --allocated-storage 20

# 4. Create MSK Kafka cluster
aws kafka create-cluster \
  --cluster-name json2fhir-kafka \
  --broker-node-group-info InstanceType=kafka.t3.small

# 5. Create ECS task definitions and services
# Use CloudFormation or manually create task definitions
```

---

## Azure Container Instances Deployment

```bash
# Create resource group
az group create --name json2fhir-rg --location eastus

# Create container registry
az acr create --resource-group json2fhir-rg --name json2fhirregistry --sku Basic

# Build and push images
az acr build --registry json2fhirregistry --image json2fhir-integration-api:1.0.0 -f integration-api/Dockerfile .

# Deploy containers
az container create \
  --resource-group json2fhir-rg \
  --name integration-api \
  --image json2fhirregistry.azurecr.io/json2fhir-integration-api:1.0.0 \
  --ports 8000 \
  --environment-variables DATABASE_URL="..." KAFKA_BOOTSTRAP_SERVERS="..."
```

---

## Google Cloud Run Deployment

```bash
# Set project ID
export PROJECT_ID=my-project

# Enable required APIs
gcloud services enable run.googleapis.com
gcloud services enable cloudsql.googleapis.com

# Build image
gcloud builds submit --tag gcr.io/$PROJECT_ID/integration-api -f integration-api/Dockerfile .

# Deploy to Cloud Run
gcloud run deploy integration-api \
  --image gcr.io/$PROJECT_ID/integration-api \
  --platform managed \
  --region us-central1 \
  --memory 512Mi \
  --set-env-vars DATABASE_URL="..." \
  --allow-unauthenticated
```

---

## Health Checks and Monitoring

### Kubernetes Health Checks

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 30
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 10
  failureThreshold: 2
```

### Prometheus Metrics (Optional)

Add to each service:
```bash
pip install prometheus-client
```

### Logging Stack

```bash
# Deploy ELK Stack
helm install elk elastic/elasticsearch -n json2fhir

# Configure log forwarding
# Update docker-compose.yml with logging driver
```

---

## Backup and Recovery

### Database Backups

```bash
# Automated backup (daily)
*/2 3 * * * /backup-db.sh

# Manual backup
mysqldump -h mysql -u fhir_user -p fhir_db > backup-$(date +%Y%m%d).sql

# Restore from backup
mysql -h mysql -u fhir_user -p fhir_db < backup-20240115.sql
```

### Kafka Message Recovery

```bash
# Reset consumer group to earliest
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group json-fhir-group \
  --reset-offsets --to-earliest --execute

# Replay messages
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic json.request \
  --from-beginning
```

---

## Performance Tuning

### Database Optimization

```sql
-- Create indexes
CREATE INDEX idx_transaction_id ON transactions(transaction_id);
CREATE INDEX idx_patient_id ON transactions(patient_id);
CREATE INDEX idx_status ON transactions(status);
CREATE INDEX idx_created_at ON transactions(created_at);

-- Connection pooling
max_connections = 100
```

### Kafka Optimization

```properties
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
log.retention.hours=168
```

### Service Optimization

```python
# In requirements.txt
gunicorn==21.2.0
gevent==23.9.1

# Run with gunicorn
gunicorn -w 4 -k gevent -b 0.0.0.0:8000 src.main:app
```

---

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker-compose logs integration-api

# Check network connectivity
docker-compose exec integration-api nc -zv kafka 9092
docker-compose exec integration-api nc -zv mysql 3306

# Restart service
docker-compose restart integration-api
```

### High Memory Usage

```bash
# Check container memory
docker stats

# Set memory limits in docker-compose.yml
```

### Kafka Message Loss

```bash
# Check replication factor
kafka-topics --describe --topic json.request --bootstrap-server kafka:9092

# Increase replication
kafka-topics --alter --topic json.request --replication-factor 3
```

---

## Next Steps

1. **Setup Monitoring**: Implement Prometheus + Grafana
2. **Add Authentication**: Implement JWT or API keys
3. **Enable HTTPS**: Configure TLS certificates
4. **Implement Caching**: Add Redis for performance
5. **Setup CI/CD**: GitHub Actions or GitLab CI
6. **Disaster Recovery**: Configure automated backups
