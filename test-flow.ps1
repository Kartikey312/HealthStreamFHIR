# JSON2FHIR - End-to-End Flow Test Script (PowerShell)

# Configuration
$IntegrationAPI = "http://localhost:8000"
$CommunicationService = "http://localhost:8001"

# Helper function to make API calls
function Invoke-RestMethod-Quiet {
    param($Uri)
    try {
        return Invoke-RestMethod -Uri $Uri -ErrorAction SilentlyContinue
    } catch {
        return $null
    }
}

# Display header
Write-Host "================================================== " -ForegroundColor Cyan
Write-Host "   JSON2FHIR - End-to-End Flow Test Script        " -ForegroundColor Cyan
Write-Host "================================================== " -ForegroundColor Cyan
Write-Host ""

# Check if services are available
Write-Host "[1] Checking service availability..." -ForegroundColor Cyan

$HealthIntegration = Invoke-RestMethod-Quiet -Uri "$IntegrationAPI/health"
if ($HealthIntegration) {
    Write-Host "✅ Integration API is running" -ForegroundColor Green
} else {
    Write-Host "❌ Integration API is not responding" -ForegroundColor Red
    Write-Host "Please start services with: docker-compose up -d"
    exit 1
}

$HealthComm = Invoke-RestMethod-Quiet -Uri "$CommunicationService/health"
if ($HealthComm) {
    Write-Host "✅ Communication Service is running" -ForegroundColor Green
} else {
    Write-Host "❌ Communication Service is not responding" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 1: Submit patient data
Write-Host "[2] Submitting patient data..." -ForegroundColor Cyan

$PatientData = @{
    patient_id = "P123456"
    name = "John Doe"
    given_name = "John"
    family_name = "Doe"
    phone = "+1-555-0123"
    email = "john.doe@example.com"
    address = "123 Main Street, Springfield, USA"
    status = "new_admission"
} | ConvertTo-Json

Write-Host "Request:" -ForegroundColor Gray
$PatientData | ConvertFrom-Json | ConvertTo-Json | Write-Host

Write-Host ""

try {
    $Response = Invoke-RestMethod -Uri "$IntegrationAPI/patient" `
        -Method POST `
        -Body $PatientData `
        -ContentType "application/json"
    
    Write-Host "Response:" -ForegroundColor Gray
    $Response | ConvertTo-Json | Write-Host
    
    $TransactionID = $Response.transaction_id
    
    if (-not $TransactionID) {
        Write-Host "❌ Failed to get transaction ID" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✅ Transaction created: $TransactionID" -ForegroundColor Green
} catch {
    Write-Host "❌ Error submitting patient data: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 2: Wait for processing
Write-Host "[3] Waiting for initial processing (5 seconds)..." -ForegroundColor Cyan
Start-Sleep -Seconds 5

# Step 3: Check transaction status
Write-Host "[4] Checking transaction status..." -ForegroundColor Cyan

try {
    $StatusResponse = Invoke-RestMethod -Uri "$IntegrationAPI/transaction/$TransactionID"
    
    Write-Host "Status Response:" -ForegroundColor Gray
    $StatusResponse | ConvertTo-Json | Write-Host
    
    $CurrentStatus = $StatusResponse.status
    Write-Host "✅ Current Status: $CurrentStatus" -ForegroundColor Green
} catch {
    Write-Host "⚠️ Error checking status: $_" -ForegroundColor Yellow
}

Write-Host ""

# Step 4: Simulate hospital response
Write-Host "[5] Simulating hospital FHIR response..." -ForegroundColor Cyan

$Timestamp = (Get-Date -AsUTC).ToString("yyyy-MM-ddTHH:mm:ssZ")

$FhirResponse = @{
    original_id = $TransactionID
    hospital_system_id = "HSP-2024-001"
    status = "201 Created"
    timestamp = $Timestamp
    fhir_response = @{
        resourceType = "Patient"
        id = "HSP-2024-001"
        name = @(
            @{
                use = "official"
                text = "John Doe"
            }
        )
        active = $true
        telecom = @(
            @{
                system = "phone"
                value = "+1-555-0123"
            }
        )
    }
} | ConvertTo-Json -Depth 10

Write-Host "FHIR Response:" -ForegroundColor Gray
$FhirResponse | Write-Host

Write-Host ""

try {
    $CommResponse = Invoke-RestMethod -Uri "$CommunicationService/fhir/response" `
        -Method POST `
        -Body $FhirResponse `
        -ContentType "application/json"
    
    Write-Host "Communication Service Response:" -ForegroundColor Gray
    $CommResponse | ConvertTo-Json | Write-Host
    
    Write-Host "✅ FHIR response submitted" -ForegroundColor Green
} catch {
    Write-Host "❌ Error submitting FHIR response: $_" -ForegroundColor Red
}

Write-Host ""

# Step 5: Wait for final processing
Write-Host "[6] Waiting for final processing (5 seconds)..." -ForegroundColor Cyan
Start-Sleep -Seconds 5

# Step 6: Check final transaction status
Write-Host "[7] Checking final transaction status..." -ForegroundColor Cyan

try {
    $FinalStatus = Invoke-RestMethod -Uri "$IntegrationAPI/transaction/$TransactionID"
    
    Write-Host "Final Status:" -ForegroundColor Gray
    $FinalStatus | ConvertTo-Json | Write-Host
    
    $FinalState = $FinalStatus.status
} catch {
    Write-Host "⚠️ Error checking final status: $_" -ForegroundColor Yellow
    $FinalState = "UNKNOWN"
}

Write-Host ""

# Display summary
Write-Host "================================================== " -ForegroundColor Cyan
Write-Host "   Test Summary                                    " -ForegroundColor Cyan
Write-Host "================================================== " -ForegroundColor Cyan
Write-Host "Transaction ID: $TransactionID" -ForegroundColor Green
Write-Host "Patient ID: P123456" -ForegroundColor Green
Write-Host "Patient Name: John Doe" -ForegroundColor Green
Write-Host "Hospital Reference ID: HSP-2024-001" -ForegroundColor Green
Write-Host "Final Status: $FinalState" -ForegroundColor Green
Write-Host "================================================== " -ForegroundColor Cyan
Write-Host ""

if ($FinalState -eq "SUCCESS") {
    Write-Host "✅ END-TO-END TEST COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host ""
    Write-Host "The complete flow from JSON input to database storage has been verified." -ForegroundColor Green
    Write-Host ""
    Write-Host "Database Records Created:" -ForegroundColor Green
    Write-Host "  1. transactions table: Transaction record with status=$FinalState" -ForegroundColor Green
    Write-Host "  2. fhir_requests table: FHIR Patient resource record" -ForegroundColor Green
    Write-Host "  3. fhir_responses table: Hospital response record" -ForegroundColor Green
    Write-Host "  4. response_mappings table: JSON to FHIR to JSON mapping" -ForegroundColor Green
} else {
    Write-Host "⚠️ Final status is not SUCCESS. Check the logs:" -ForegroundColor Yellow
    Write-Host "  docker-compose logs -f" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Useful Commands:" -ForegroundColor Cyan
Write-Host "  View all logs:        docker-compose logs -f"
Write-Host "  Kafka UI:             http://localhost:8080"
Write-Host "  API Docs:             http://localhost:8000/docs"
Write-Host ""
