require 'rspec'
require 'pg'
require 'dotenv'
require_relative '../lambda_function'

RSpec.describe 'ResourceDeleter Integration Tests', :integration do
  let(:deleter) { ResourceDeleter.new }
  let(:db_config) do
    {
      host: ENV['FHM_REPORTINGDB_HOST'] || 'localhost',
      port: ENV['FHM_REPORTINGDB_PORT'] || 5432,
      dbname: ENV['FHM_REPORTINGDB_NAME'] || 'reportingdb',
      user: ENV['FHM_REPORTINGDB_USER'] || 'postgres',
      password: ENV['FHM_REPORTINGDB_PASSWORD'] || 'Test12345'
    }
  end

  let(:test_connection) { PG.connect(db_config) }

  before(:all) do
    Dotenv.load('.env.test') if File.exist?('.env.test')
    # Skip integration tests if database environment variables are not set
    skip 'Integration tests require database configuration' unless database_available?
  end

  after(:all) do
  end

  before(:each) do
    skip 'Integration tests require database configuration' unless database_available?
    cleanup_test_data

    # Override environment variables for the test
    allow(ENV).to receive(:[]).and_call_original
    allow(ENV).to receive(:[]).with('DB_HOST').and_return(db_config[:host])
    allow(ENV).to receive(:[]).with('DB_PORT').and_return(db_config[:port].to_s)
    allow(ENV).to receive(:[]).with('DB_NAME').and_return(db_config[:dbname])
    allow(ENV).to receive(:[]).with('DB_USER').and_return(db_config[:user])
    allow(ENV).to receive(:[]).with('DB_PASSWORD').and_return(db_config[:password])
  end

  def setup_cascading_test_data(test_prefix)

    managing_org_result = test_connection.exec_params(
      'INSERT INTO dim_organization (mongo_id, name) VALUES ($1, $2) RETURNING id',
      ["#{test_prefix}_org_001", 'Health Org']
    )
    managing_org_id = managing_org_result[0]['id']

    person_result = test_connection.exec_params(
      "INSERT INTO dim_person (mongo_id, first_name, last_name, dim_birth_date_fk, managing_organization_fk)
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
      ["#{test_prefix}_person_001", 'John', 'Doe', 20000101, managing_org_id]
    )
    person_id = person_result[0]['id']

    product_result = test_connection.exec_params(
      'INSERT INTO dim_product (mongo_id, name, managing_organization_fk) VALUES ($1, $2, $3) RETURNING id',
      ["#{test_prefix}_product_001", 'Standard Product', managing_org_id]
    )
    product_id = product_result[0]['id']

    medical_result = test_connection.exec_params(
      "INSERT INTO fact_medical (
          mongo_id, dim_person_fk, dim_product_fk, dim_managing_organization_fk,
          dim_medical_date_fk, dim_medical_time_fk, status, client_age_at_medical,
          phone_call_required)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id",
      ["#{test_prefix}_medical_101", person_id, product_id, managing_org_id, 20250101, 90000, 'dispatched', 25, false]
    )
    medical_id = medical_result[0]['id']

    # Insert related records
    health_category_report_result = test_connection.exec_params(
      "INSERT INTO fact_health_category_report (
          mongo_id, dim_person_fk, fact_medical_fk, dim_product_fk, dim_managing_organization_fk,
          dim_report_date_fk, dim_report_time_fk, name, flag, phone_call_required)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id",
      ["#{test_prefix}_hc_001", person_id, medical_id, product_id, managing_org_id, 20250102, 90000, 'General Health', 'green', false]
    )
    health_category_report_result_id = health_category_report_result[0]['id']

    observation_result = test_connection.exec_params(
      "INSERT INTO fact_observation (
          mongo_id, dim_person_fk, fact_medical_fk, dim_product_fk, dim_managing_organization_fk, name, label, category, section, valueType)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id",
      ["#{test_prefix}_observation_001", person_id, medical_id, product_id, managing_org_id, 'observation_001', 'Blood Pressure', 'Vital', 'Vitals', 'Numeric' ]
    )
    observation_result_id = observation_result[0]['id']

    contract_result = test_connection.exec_params(
      'INSERT INTO dim_contract (mongo_id, name, managing_organization_id, recipient_organization_id, contract_code)
       VALUES ($1, $2, $3, $4, $5) RETURNING id',
      ["#{test_prefix}_contract_001", 'Standard Contract', managing_org_id, managing_org_id, "#{test_prefix}CON123"]
    )
    contract_id = contract_result[0]['id']

    people_enrollment_result = test_connection.exec_params(
      "INSERT INTO fact_people_enrollment (
        mongo_id, dim_person_fk, dim_contract_fk, dim_recipient_organization_fk, dim_managing_organization_fk, status)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
      ["#{test_prefix}_enrollment_001", person_id, contract_id, managing_org_id, managing_org_id, 'active']
    )
    enrollment_id = people_enrollment_result[0]['id']

    {
      managing_org_id: managing_org_id,
      person_id: person_id,
      product_id: product_id,
      medical_id: medical_id,
      health_category_report_id: health_category_report_result_id,
      observation_id: observation_result_id,
      contract_id: contract_id,
      enrollment_id: enrollment_id
    }
  end

  describe 'standard collection deletions' do
    it 'deletes organization record successfully' do
      # Setup test data
      test_connection.exec_params(
        'INSERT INTO dim_organization (mongo_id, name) VALUES ($1, $2)',
        ['test_org_456', 'Test Organization']
      )

      message = { 'collection' => 'companies', 'resource_id' => 'test_org_456' }

      expect { deleter.process_message(message) }.not_to raise_error

      # Verify deletion
      result = test_connection.exec_params(
        'SELECT COUNT(*) FROM dim_organization WHERE mongo_id = $1',
        ['test_org_456']
      )
      expect(result[0]['count']).to eq('0')
    end
  end

  describe 'people collection with cascading deletions' do
    it 'deletes person and related enrollment records' do

      # Setup test data
      ids = setup_cascading_test_data "people"
      person_id = ids[:person_id]
      resource_id = 'people_person_001'

      message = { 'collection' => 'people', 'resource_id' => resource_id }

      expect { deleter.process_message(message) }.not_to raise_error

      # Verify person deletion
      person_count = test_connection.exec_params(
        'SELECT COUNT(*) FROM dim_person WHERE mongo_id = $1',
        [resource_id]
      )
      expect(person_count[0]['count']).to eq('0')

      # Verify enrollment deletions
      enrollment_count = test_connection.exec_params(
        'SELECT COUNT(*) FROM fact_people_enrollment WHERE dim_person_fk = $1',
        [person_id]
      )
      expect(enrollment_count[0]['count']).to eq('0')
    end

    it 'handles non-existent person gracefully' do
      message = { 'collection' => 'people', 'resource_id' => 'non_existent_person' }

      expect { deleter.process_message(message) }.not_to raise_error
    end
  end

  describe 'medicals collection with cascading deletions' do
    it 'deletes medical record and related records' do
      # Setup test data
      ids = setup_cascading_test_data 'medicals'
      medical_id = ids[:medical_id]
      resource_id = 'medicals_medical_101'

      message = { 'collection' => 'medicals', 'resource_id' => resource_id }

      expect { deleter.process_message(message) }.not_to raise_error

      # Verify medical deletion
      medical_count = test_connection.exec_params(
        'SELECT COUNT(*) FROM fact_medical WHERE mongo_id = $1',
        [resource_id]
      )
      expect(medical_count[0]['count']).to eq('0')

      # Verify related record deletions
      health_category_count = test_connection.exec_params(
        'SELECT COUNT(*) FROM fact_health_category_report WHERE fact_medical_fk = $1',
        [medical_id]
      )
      expect(health_category_count[0]['count']).to eq('0')

      observation_count = test_connection.exec_params(
        'SELECT COUNT(*) FROM fact_observation WHERE fact_medical_fk = $1',
        [medical_id]
      )
      expect(observation_count[0]['count']).to eq('0')
    end

    it 'handles non-existent medical record gracefully' do
      message = { 'collection' => 'medicals', 'resource_id' => 'non_existent_medical' }

      expect { deleter.process_message(message) }.not_to raise_error
    end
  end

  private

  def database_available?
    ENV['FHM_REPORTINGDB_HOST'] && ENV['FHM_REPORTINGDB_USER'] && ENV['FHM_REPORTINGDB_PASSWORD']
  end

  def cleanup_test_data
    tables = %w[
      fact_observation fact_health_category_report fact_people_enrollment
      fact_medical dim_person dim_programme dim_product dim_location
      dim_group dim_contract dim_organization fact_appointment
    ]

    tables.each do |table|
      test_connection.exec("TRUNCATE TABLE #{table} RESTART IDENTITY CASCADE")
    end
  end
end
