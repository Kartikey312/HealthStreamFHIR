-- Initialize FHIR Database

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
  id INT PRIMARY KEY AUTO_INCREMENT,
  transaction_id VARCHAR(255) UNIQUE NOT NULL,
  patient_id VARCHAR(255) NOT NULL,
  patient_name VARCHAR(255),
  status ENUM('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED') DEFAULT 'PENDING',
  json_payload LONGTEXT,
  fhir_payload LONGTEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_patient_id (patient_id),
  INDEX idx_status (status),
  INDEX idx_created_at (created_at)
);

-- Create fhir_requests table
CREATE TABLE IF NOT EXISTS fhir_requests (
  id INT PRIMARY KEY AUTO_INCREMENT,
  transaction_id VARCHAR(255) NOT NULL,
  request_id VARCHAR(255) UNIQUE NOT NULL,
  fhir_resource_type VARCHAR(100),
  fhir_payload LONGTEXT,
  validation_status ENUM('PENDING', 'VALID', 'INVALID') DEFAULT 'PENDING',
  validation_errors LONGTEXT,
  sent_to_hospital BOOLEAN DEFAULT FALSE,
  sent_at TIMESTAMP NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
  INDEX idx_transaction_id (transaction_id),
  INDEX idx_validation_status (validation_status)
);

-- Create fhir_responses table
CREATE TABLE IF NOT EXISTS fhir_responses (
  id INT PRIMARY KEY AUTO_INCREMENT,
  transaction_id VARCHAR(255) NOT NULL,
  response_id VARCHAR(255) UNIQUE NOT NULL,
  fhir_payload LONGTEXT,
  hospital_response_code INT,
  hospital_response_message VARCHAR(500),
  received_at TIMESTAMP,
  processed BOOLEAN DEFAULT FALSE,
  processed_at TIMESTAMP NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
  INDEX idx_transaction_id (transaction_id),
  INDEX idx_received_at (received_at)
);

-- Create response_mappings table for JSON->FHIR tracking
CREATE TABLE IF NOT EXISTS response_mappings (
  id INT PRIMARY KEY AUTO_INCREMENT,
  transaction_id VARCHAR(255) NOT NULL,
  original_json LONGTEXT,
  final_json LONGTEXT,
  status ENUM('PENDING', 'COMPLETED', 'FAILED') DEFAULT 'PENDING',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
  INDEX idx_transaction_id (transaction_id)
);
