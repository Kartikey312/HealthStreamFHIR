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

-- =====================================================================================
-- PreAuth claim tables - MySQL port of the 11 tables read by the SQL Server SP
-- USP_Get_PreAuthClaimsDetails_ByClaimId.
--
-- diagnosis / care_team / coverage_class / claim_request_encounter are read via
-- SELECT * in the original SP, so their real column list isn't visible anywhere
-- in it - only the join key used to reach them is known. They're stubs (id + join
-- key only) until the real DDL is available.
-- =====================================================================================

CREATE TABLE IF NOT EXISTS claim (
  id                                    VARCHAR(200)   NOT NULL PRIMARY KEY,
  accident_date                         DATETIME       NULL,
  accident_location_adress              VARCHAR(500)   NULL,
  accident_type                         VARCHAR(100)   NULL,
  billable_end                          DATETIME       NULL,
  billable_start                        DATETIME       NULL,
  bundle_id                             VARCHAR(200)   NULL,
  consumed                              TINYINT(1)     NULL,
  created                               DATETIME       NULL,
  diagnosis_related_group               VARCHAR(100)   NULL,
  eligibility_off_line                  VARCHAR(50)    NULL,
  eligibility_off_line_date             DATETIME       NULL,
  eligibility_response_identifier       VARCHAR(200)   NULL,
  encounter_class                       VARCHAR(100)   NULL,
  encounter_end                         DATETIME       NULL,
  encounter_identifier                  VARCHAR(200)   NULL,
  encounter_start                       DATETIME       NULL,
  facility                              VARCHAR(255)   NULL,
  identifier                            VARCHAR(200)   NULL,
  identifier_system                     VARCHAR(255)   NULL,
  inserted_on                           DATETIME       NULL,
  inserted_on_date_time                 DATETIME       NULL,
  insurer_identifier                    VARCHAR(100)   NULL,
  insurer_identifier_system             VARCHAR(255)   NULL,
  khatm                                 VARCHAR(50)    NULL,
  new_born                              TINYINT(1)     NULL,
  patient_identifier                    VARCHAR(100)   NULL,
  patient_identifier_system             VARCHAR(255)   NULL,
  patient_identifier_type               VARCHAR(50)    NULL,
  payee_party                           VARCHAR(100)   NULL,
  payee_type                            VARCHAR(100)   NULL,
  prescriber_identifier                 VARCHAR(100)   NULL,
  prescription_identifier               VARCHAR(100)   NULL,
  priority                              VARCHAR(50)    NULL,
  provider_identifier                   VARCHAR(100)   NULL,
  referral                              VARCHAR(200)   NULL,
  resource_type                         VARCHAR(100)   NULL,
  status                                VARCHAR(50)    NULL,
  sub_type                              VARCHAR(100)   NULL,
  total                                 DECIMAL(18, 3) NULL,
  type                                  VARCHAR(100)   NULL,
  `use`                                 VARCHAR(50)    NULL,
  message_header_id                     VARCHAR(200)   NULL,
  provider_name                         VARCHAR(255)   NULL,
  insurer_name                          VARCHAR(255)   NULL,
  patient_birth_date                    DATETIME       NULL,
  patient_gender                        VARCHAR(20)    NULL,
  patient_name                          VARCHAR(255)   NULL,
  patient_telecom                       VARCHAR(100)   NULL,
  order_category                        VARCHAR(100)   NULL,
  order_reference                       VARCHAR(200)   NULL,
  INDEX idx_claim_identifier (identifier),
  INDEX idx_claim_elig_resp_id (eligibility_response_identifier),
  INDEX idx_claim_message_header (message_header_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS message_header (
  id                                     VARCHAR(200)  NOT NULL PRIMARY KEY,
  destination_receiver_identifier        VARCHAR(200)  NULL,
  event_coding                           VARCHAR(100)  NULL,
  focus                                  VARCHAR(500)  NULL,
  response_code                          VARCHAR(50)   NULL,
  response_identifier                    VARCHAR(200)  NULL,
  sender_identifier                      VARCHAR(200)  NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS claim_related (
  id                  VARCHAR(200)  NOT NULL PRIMARY KEY,
  claim_identifier    VARCHAR(200)  NULL,
  relationship        VARCHAR(100)  NULL,
  claim_id            VARCHAR(200)  NOT NULL,
  INDEX idx_claim_related_claim_id (claim_id),
  CONSTRAINT fk_claim_related_claim FOREIGN KEY (claim_id) REFERENCES claim (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS claim_request_insurance (
  id                              VARCHAR(200)   NOT NULL PRIMARY KEY,
  coverage_identifier             VARCHAR(200)   NULL,
  coverage_subscriber_identifier  VARCHAR(200)   NULL,
  focal                           TINYINT(1)     NULL,
  sequence                        INT            NULL,
  claim_id                        VARCHAR(200)   NOT NULL,
  coverage_subscriber_system      VARCHAR(255)   NULL,
  coverage_beneficiary_system     VARCHAR(255)   NULL,
  INDEX idx_cri_claim_id (claim_id),
  CONSTRAINT fk_claim_request_insurance_claim FOREIGN KEY (claim_id) REFERENCES claim (id)
) ENGINE=InnoDB;

-- *** SCHEMA UNKNOWN (SP reads via SELECT *) - join-key stub only ***
CREATE TABLE IF NOT EXISTS diagnosis (
  id          VARCHAR(200)  NOT NULL PRIMARY KEY,
  claim_id    VARCHAR(200)  NOT NULL,
  INDEX idx_diagnosis_claim_id (claim_id),
  CONSTRAINT fk_diagnosis_claim FOREIGN KEY (claim_id) REFERENCES claim (id)
) ENGINE=InnoDB;

-- *** SCHEMA UNKNOWN (SP reads via SELECT *) - join-key stub only ***
CREATE TABLE IF NOT EXISTS care_team (
  id          VARCHAR(200)  NOT NULL PRIMARY KEY,
  claim_id    VARCHAR(200)  NOT NULL,
  INDEX idx_care_team_claim_id (claim_id),
  CONSTRAINT fk_care_team_claim FOREIGN KEY (claim_id) REFERENCES claim (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS supporting_info (
  id                                VARCHAR(200)    NOT NULL PRIMARY KEY,
  category                         VARCHAR(100)    NULL,
  code                             VARCHAR(100)    NULL,
  reason                           VARCHAR(200)    NULL,
  sequence                         INT             NULL,
  timing_date                      DATETIME        NULL,
  timing_end                       DATETIME        NULL,
  timing_start                     DATETIME        NULL,
  value_attachment_content_type    VARCHAR(100)    NULL,
  value_attachment_data            LONGTEXT        NULL,
  value_attachment_title           VARCHAR(255)    NULL,
  value_attachment_url             VARCHAR(500)    NULL,
  value_boolean                    TINYINT(1)      NULL,
  value_quantity                   DECIMAL(18, 3)  NULL,
  value_string                     VARCHAR(500)    NULL,
  claim_id                         VARCHAR(200)    NOT NULL,
  supporting_info_code             VARCHAR(100)    NULL,
  supporting_info_display          VARCHAR(255)    NULL,
  display                          VARCHAR(255)    NULL,
  text                             LONGTEXT        NULL,
  INDEX idx_supporting_info_claim_id (claim_id),
  CONSTRAINT fk_supporting_info_claim FOREIGN KEY (claim_id) REFERENCES claim (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS claim_request_item (
  id                                VARCHAR(200)    NOT NULL PRIMARY KEY,
  batch_number                     VARCHAR(100)    NULL,
  body_site                        VARCHAR(100)    NULL,
  care_team_sequence               INT             NULL,
  diagnosis_sequence               INT             NULL,
  expiry_date                      DATETIME        NULL,
  factor                           DECIMAL(18, 3)  NULL,
  information_sequence             INT             NULL,
  net                              DECIMAL(18, 3)  NULL,
  package_extenstion               TINYINT(1)      NULL,
  patient_share                    DECIMAL(18, 3)  NULL,
  payer_share                      DECIMAL(18, 3)  NULL,
  product_or_service_code          VARCHAR(100)    NULL,
  product_or_service_system        VARCHAR(255)    NULL,
  quantity                         DECIMAL(18, 3)  NULL,
  sequence                         INT             NULL,
  serial_number                    VARCHAR(100)    NULL,
  serviced_date                    DATETIME        NULL,
  serviced_end                     DATETIME        NULL,
  serviced_start                   DATETIME        NULL,
  sub_site                         VARCHAR(100)    NULL,
  tax                              DECIMAL(18, 3)  NULL,
  teleconsultation                 TINYINT(1)      NULL,
  unit_price                       DECIMAL(18, 3)  NULL,
  claim_id                         VARCHAR(200)    NOT NULL,
  product_or_service_description   VARCHAR(500)    NULL,
  payer_code                       VARCHAR(100)    NULL,
  payer_code_display               VARCHAR(255)    NULL,
  payer_code_system                VARCHAR(255)    NULL,
  INDEX idx_claim_request_item_claim_id (claim_id),
  CONSTRAINT fk_claim_request_item_claim FOREIGN KEY (claim_id) REFERENCES claim (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS claim_request_detail (
  id                               VARCHAR(200)    NOT NULL PRIMARY KEY,
  net                              DECIMAL(18, 3)  NULL,
  product_or_service_code          VARCHAR(100)    NULL,
  product_or_service_system        VARCHAR(255)    NULL,
  quantity                         DECIMAL(18, 3)  NULL,
  sequence                         INT             NULL,
  tax                              DECIMAL(18, 3)  NULL,
  unit_price                       DECIMAL(18, 3)  NULL,
  item_id                          VARCHAR(200)    NOT NULL,
  product_or_service_description   VARCHAR(500)    NULL,
  INDEX idx_claim_request_detail_item_id (item_id),
  CONSTRAINT fk_claim_request_detail_item FOREIGN KEY (item_id) REFERENCES claim_request_item (id)
) ENGINE=InnoDB;

-- *** SCHEMA UNKNOWN (SP reads via SELECT *) - join-key stub only ***
CREATE TABLE IF NOT EXISTS coverage_class (
  id            VARCHAR(200)  NOT NULL PRIMARY KEY,
  insurance_id  VARCHAR(200)  NOT NULL,
  INDEX idx_coverage_class_insurance_id (insurance_id),
  CONSTRAINT fk_coverage_class_insurance FOREIGN KEY (insurance_id) REFERENCES claim_request_insurance (id)
) ENGINE=InnoDB;

-- *** SCHEMA UNKNOWN (SP reads via SELECT *) - join-key stub only ***
CREATE TABLE IF NOT EXISTS claim_request_encounter (
  id            VARCHAR(200)  NOT NULL PRIMARY KEY,
  identifier    VARCHAR(200)  NOT NULL,
  INDEX idx_claim_request_encounter_identifier (identifier)
) ENGINE=InnoDB;

-- =====================================================================================
-- usp_get_preauth_claims_details_by_claim_id
-- MySQL port of dbo.USP_Get_PreAuthClaimsDetails_ByClaimId, @AsJson = 1 mode only.
-- Resolves claim id -> identifier -> eligibility_response_identifier, then returns one
-- JSON object with the same 11 array keys the SQL Server SP produces.
--
-- NOT ported: the Tosfa (NlgicMedical_OMAN_New) member/policy enrichment - a separate
-- external system with its own unknown schema. Claims come back without the
-- Fromdate/Policyno/MemberNo/etc. fields the original SP fills in when
-- @IncludeTosfa = 1; this mirrors @IncludeTosfa = 0 behavior.
-- =====================================================================================
DROP PROCEDURE IF EXISTS usp_get_preauth_claims_details_by_claim_id;

DELIMITER $$

CREATE PROCEDURE usp_get_preauth_claims_details_by_claim_id(
  IN p_claim_id VARCHAR(200)
)
BEGIN
  DECLARE v_resolved_id VARCHAR(200) DEFAULT NULL;

  SELECT id INTO v_resolved_id FROM claim WHERE id = p_claim_id LIMIT 1;

  IF v_resolved_id IS NULL THEN
    SELECT id INTO v_resolved_id FROM claim
    WHERE identifier = p_claim_id
    ORDER BY inserted_on_date_time DESC LIMIT 1;
  END IF;

  IF v_resolved_id IS NULL THEN
    SELECT id INTO v_resolved_id FROM claim
    WHERE eligibility_response_identifier = p_claim_id
    ORDER BY inserted_on_date_time DESC LIMIT 1;
  END IF;

  SELECT JSON_OBJECT(
    'MessageHeaders', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'id', mh.id,
        'destination_receiver_identifier', mh.destination_receiver_identifier,
        'event_coding', mh.event_coding,
        'focus', mh.focus,
        'response_code', mh.response_code,
        'response_identifier', mh.response_identifier,
        'sender_identifier', mh.sender_identifier
      ))
      FROM message_header mh
      INNER JOIN claim c ON c.message_header_id = mh.id
      WHERE c.id = v_resolved_id
    ),
    'Claims', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'id', c.id,
        'accident_date', c.accident_date,
        'accident_location_adress', c.accident_location_adress,
        'accident_type', c.accident_type,
        'billable_end', c.billable_end,
        'billable_start', c.billable_start,
        'bundle_id', c.bundle_id,
        'consumed', c.consumed,
        'created', c.created,
        'diagnosis_related_group', c.diagnosis_related_group,
        'eligibility_off_line', c.eligibility_off_line,
        'eligibility_off_line_date', c.eligibility_off_line_date,
        'eligibility_response_identifier', c.eligibility_response_identifier,
        'eligibility_response_identifier_system', '',
        'encounter_class', c.encounter_class,
        'encounter_end', COALESCE(c.encounter_end, c.billable_end),
        'encounter_identifier', c.encounter_identifier,
        'encounter_identifier_system', '',
        'encounter_start', COALESCE(c.encounter_start, c.billable_start),
        'facility', c.facility,
        'identifier', c.identifier,
        'identifier_system', c.identifier_system,
        'inserted_on', c.inserted_on,
        'insurer_identifier', c.insurer_identifier,
        'insurer_identifier_system', c.insurer_identifier_system,
        'khatm', c.khatm,
        'new_born', c.new_born,
        'patient_identifier', c.patient_identifier,
        'patient_identifier_system', c.patient_identifier_system,
        'patient_identifier_type', c.patient_identifier_type,
        'payee_party', c.payee_party,
        'payee_type', c.payee_type,
        'prescriber_identifier', c.prescriber_identifier,
        'prescriber_identifier_system', '',
        'prescription_identifier', c.prescription_identifier,
        'prescription_identifier_system', '',
        'priority', c.priority,
        'provider_identifier', c.provider_identifier,
        'provider_identifier_system', '',
        'referral', c.referral,
        'resource_type', c.resource_type,
        'status', c.status,
        'sub_type', c.sub_type,
        'total', c.total,
        'type', c.type,
        'use', c.`use`,
        'message_header_id', c.message_header_id,
        'provider_name', c.provider_name,
        'insurer_name', CASE WHEN c.provider_identifier IN ('PRV-1538', 'PRV-3306', 'PRV-330')
                             THEN c.insurer_name ELSE 'LIVA INSURANCE' END,
        'PreAuthId', CONCAT('~', c.eligibility_response_identifier),
        'patient_birth_date', c.patient_birth_date,
        'patient_gender', c.patient_gender,
        'patient_name', c.patient_name,
        'patient_telecom', c.patient_telecom,
        'order_category', c.order_category,
        'order_reference', c.order_reference,
        'Fromdate', NULL, 'ToDate', NULL, 'Policyno', 0, 'ClassCode', 0,
        'MemberNo', 0, 'MemberName', '', 'IsVip', 0, 'IsExclusive', 0,
        'MemberBand', '', 'MobileNo', '', 'MemberType', NULL,
        'PlanTemplateCode', 0, 'deductableType', NULL, 'cardnumber', NULL,
        'Ins_code', 0, 'ClientNo', 0, 'ClientRefNo', 0, 'FollowUpDays', 0,
        'ProcessStatus', 0, 'FailReason', NULL
      ))
      FROM claim c
      WHERE c.id = v_resolved_id
    ),
    'ClaimRelateds', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'id', cr.id,
        'claim_identifier', cr.claim_identifier,
        'claim_identifier_system', NULL,
        'relationship', cr.relationship,
        'claim_id', cr.claim_id,
        'RequestNo', NULL
      ))
      FROM claim_related cr
      WHERE cr.claim_id = v_resolved_id
    ),
    'ClaimRequestInsurances', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'id', ci.id,
        'coverage_identifier', ci.coverage_identifier,
        'coverage_identifier_system', NULL,
        'coverage_subscriber', NULL,
        'focal', ci.focal,
        'sequence', ci.sequence,
        'claim_id', ci.claim_id,
        'coverage_subscriber_system', ci.coverage_subscriber_system,
        'coverage_beneficiary_system', ci.coverage_beneficiary_system
      ))
      FROM claim_request_insurance ci
      WHERE ci.claim_id = v_resolved_id
    ),
    'Diagnosiss', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT('id', d.id, 'claim_id', d.claim_id))
      FROM diagnosis d
      WHERE d.claim_id = v_resolved_id
    ),
    'CareTeams', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT('id', ct.id, 'claim_id', ct.claim_id))
      FROM care_team ct
      WHERE ct.claim_id = v_resolved_id
    ),
    'Supportinginfo', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'id', si.id,
        'category', si.category,
        'code', si.code,
        'reason', si.reason_numbered,
        'sequence', si.sequence,
        'timing_date', si.timing_date,
        'timing_end', si.timing_end,
        'timing_start', si.timing_start,
        'value_attachment_content_type', si.value_attachment_content_type,
        'value_attachment_data', si.value_attachment_data,
        'value_attachment_title', si.value_attachment_title,
        'value_attachment_url', si.value_attachment_url,
        'value_boolean', si.value_boolean = 1,
        'value_quantity', si.value_quantity,
        'value_string', si.value_string,
        'claim_id', si.claim_id,
        'supporting_info_code', si.supporting_info_code,
        'supporting_info_display', si.supporting_info_display,
        'display', si.display,
        'text', si.text
      ))
      FROM (
        SELECT s.*,
               CONCAT(s.reason, '_',
                   ROW_NUMBER() OVER (PARTITION BY s.claim_id ORDER BY s.id)) AS reason_numbered
        FROM supporting_info s
        WHERE s.claim_id = v_resolved_id
      ) si
    ),
    'ClaimRequestItems', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'id', cri.id,
        'batch_number', cri.batch_number,
        'body_site', cri.body_site,
        'care_team_sequence', cri.care_team_sequence,
        'diagnosis_sequence', cri.diagnosis_sequence,
        'expiry_date', cri.expiry_date,
        'factor', cri.factor,
        'information_sequence', cri.information_sequence,
        'net', cri.net,
        'package_extenstion', cri.package_extenstion,
        'patient_share', cri.patient_share,
        'payer_share', cri.payer_share,
        'product_or_service_code', cri.product_or_service_code,
        'product_or_service_system', cri.product_or_service_system,
        'quantity', cri.quantity,
        'sequence', cri.sequence,
        'serial_number', cri.serial_number,
        'serviced_date', cri.serviced_date,
        'serviced_end', cri.serviced_end,
        'serviced_start', cri.serviced_start,
        'sub_site', cri.sub_site,
        'tax', cri.tax,
        'teleconsultation', cri.teleconsultation,
        'unit_price', cri.unit_price,
        'claim_id', cri.claim_id,
        'product_or_service_description', cri.product_or_service_description,
        'payer_code', COALESCE(cri.payer_code, ''),
        'payer_code_display', COALESCE(cri.payer_code_display, ''),
        'payer_code_system', cri.payer_code_system
      ))
      FROM claim_request_item cri
      WHERE cri.claim_id = v_resolved_id
    ),
    'ClaimRequestDetail', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'id', crd.id,
        'net', crd.net,
        'product_or_service_code', crd.product_or_service_code,
        'product_or_service_system', crd.product_or_service_system,
        'quantity', crd.quantity,
        'sequence', crd.sequence,
        'tax', crd.tax,
        'unit_price', crd.unit_price,
        'item_id', crd.item_id,
        'product_or_service_description', crd.product_or_service_description
      ))
      FROM claim_request_detail crd
      INNER JOIN claim_request_item cri2 ON cri2.id = crd.item_id
      WHERE cri2.claim_id = v_resolved_id
    ),
    'CoverageClass', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT('id', cc.id, 'insurance_id', cc.insurance_id))
      FROM coverage_class cc
      INNER JOIN claim_request_insurance ci2 ON ci2.id = cc.insurance_id
      WHERE ci2.claim_id = v_resolved_id
    ),
    'claimRequestEncounter', (
      SELECT JSON_ARRAYAGG(JSON_OBJECT('id', cre.id, 'identifier', cre.identifier))
      FROM claim_request_encounter cre
      INNER JOIN claim c2 ON c2.encounter_identifier = cre.identifier
      WHERE c2.id = v_resolved_id
    )
  ) AS PreAuthJSON;
END$$

DELIMITER ;
