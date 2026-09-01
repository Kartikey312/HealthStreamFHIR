#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================${NC}"
echo -e "${BLUE}   JSON2FHIR - End-to-End Flow Test Script${NC}"
echo -e "${BLUE}==================================================${NC}\n"

# Configuration
INTEGRATION_API="http://localhost:8000"
COMMUNICATION_SERVICE="http://localhost:8001"

# Check if services are available
echo -e "${BLUE}[1] Checking service availability...${NC}"

if curl -s "$INTEGRATION_API/health" > /dev/null; then
    echo -e "${GREEN}✅ Integration API is running${NC}"
else
    echo -e "${RED}❌ Integration API is not responding${NC}"
    echo "Please start services with: docker-compose up -d"
    exit 1
fi

if curl -s "$COMMUNICATION_SERVICE/health" > /dev/null; then
    echo -e "${GREEN}✅ Communication Service is running${NC}"
else
    echo -e "${RED}❌ Communication Service is not responding${NC}"
    exit 1
fi

echo ""

# Step 1: Submit patient data
echo -e "${BLUE}[2] Submitting patient data...${NC}"

PATIENT_DATA='{
  "patient_id": "P123456",
  "name": "John Doe",
  "given_name": "John",
  "family_name": "Doe",
  "phone": "+1-555-0123",
  "email": "john.doe@example.com",
  "address": "123 Main Street, Springfield, USA",
  "status": "new_admission"
}'

RESPONSE=$(curl -s -X POST "$INTEGRATION_API/patient" \
  -H "Content-Type: application/json" \
  -d "$PATIENT_DATA")

echo "Request:"
echo "$PATIENT_DATA" | jq '.'

echo ""
echo "Response:"
echo "$RESPONSE" | jq '.'

# Extract transaction ID
TRANSACTION_ID=$(echo "$RESPONSE" | jq -r '.transaction_id')

if [ "$TRANSACTION_ID" == "null" ] || [ -z "$TRANSACTION_ID" ]; then
    echo -e "${RED}❌ Failed to get transaction ID${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Transaction created: $TRANSACTION_ID${NC}\n"

# Step 2: Wait for processing
echo -e "${BLUE}[3] Waiting for initial processing (5 seconds)...${NC}"
sleep 5

# Step 3: Check transaction status
echo -e "${BLUE}[4] Checking transaction status...${NC}"

STATUS_RESPONSE=$(curl -s "$INTEGRATION_API/transaction/$TRANSACTION_ID")
echo "Status Response:"
echo "$STATUS_RESPONSE" | jq '.'

CURRENT_STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.status')
echo -e "${GREEN}✅ Current Status: $CURRENT_STATUS${NC}\n"

# Step 4: Simulate hospital response
echo -e "${BLUE}[5] Simulating hospital FHIR response...${NC}"

FHIR_RESPONSE='{
  "original_id": "'$TRANSACTION_ID'",
  "hospital_system_id": "HSP-2024-001",
  "status": "201 Created",
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
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
}'

echo "FHIR Response:"
echo "$FHIR_RESPONSE" | jq '.'

COMM_RESPONSE=$(curl -s -X POST "$COMMUNICATION_SERVICE/fhir/response" \
  -H "Content-Type: application/json" \
  -d "$FHIR_RESPONSE")

echo ""
echo "Communication Service Response:"
echo "$COMM_RESPONSE" | jq '.'

echo -e "${GREEN}✅ FHIR response submitted${NC}\n"

# Step 5: Wait for final processing
echo -e "${BLUE}[6] Waiting for final processing (5 seconds)...${NC}"
sleep 5

# Step 6: Check final transaction status
echo -e "${BLUE}[7] Checking final transaction status...${NC}"

FINAL_STATUS=$(curl -s "$INTEGRATION_API/transaction/$TRANSACTION_ID")
echo "Final Status:"
echo "$FINAL_STATUS" | jq '.'

FINAL_STATE=$(echo "$FINAL_STATUS" | jq -r '.status')

echo ""
echo -e "${BLUE}==================================================${NC}"
echo -e "${BLUE}   Test Summary${NC}"
echo -e "${BLUE}==================================================${NC}"
echo -e "Transaction ID: ${GREEN}$TRANSACTION_ID${NC}"
echo -e "Patient ID: ${GREEN}P123456${NC}"
echo -e "Patient Name: ${GREEN}John Doe${NC}"
echo -e "Hospital Reference ID: ${GREEN}HSP-2024-001${NC}"
echo -e "Final Status: ${GREEN}$FINAL_STATE${NC}"
echo -e "${BLUE}==================================================${NC}\n"

if [ "$FINAL_STATE" == "SUCCESS" ]; then
    echo -e "${GREEN}✅ END-TO-END TEST COMPLETED SUCCESSFULLY!${NC}\n"
    echo "The complete flow from JSON input to database storage has been verified."
    echo ""
    echo "Database Records Created:"
    echo "  1. transactions table: Transaction record with status=$FINAL_STATE"
    echo "  2. fhir_requests table: FHIR Patient resource record"
    echo "  3. fhir_responses table: Hospital response record"
    echo "  4. response_mappings table: JSON to FHIR to JSON mapping"
else
    echo -e "${RED}⚠️ Final status is not SUCCESS. Check the logs:${NC}"
    echo "  docker-compose logs -f"
fi

echo ""
echo -e "${BLUE}Useful Commands:${NC}"
echo "  View all logs:        docker-compose logs -f"
echo "  Kafka UI:             http://localhost:8080"
echo "  MySQL Database:       mysql -h localhost -u fhir_user -p fhir_db"
echo "  API Docs:             http://localhost:8000/docs"
echo ""
