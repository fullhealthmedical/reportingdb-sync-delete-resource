require 'rspec'
require 'pg'
require_relative '../reportingdb_sync_delete_resource'

RSpec.describe ResourceDeleter do
  let(:deleter) { ResourceDeleter.new }
  let(:mock_connection) { instance_double(PG::Connection) }
  let(:mock_result) { instance_double(PG::Result) }

  before do
    allow(PG).to receive(:connect).and_return(mock_connection)
    allow(deleter).to receive(:db_connection).and_return(mock_connection)
  end

  describe '#process_message' do
    context 'with valid standard collection message' do
      let(:message) { { 'collection' => 'appointments', 'resource_id' => '123' } }

      it 'processes the message successfully' do
        allow(mock_connection).to receive(:transaction).and_yield(mock_connection)
        allow(mock_connection).to receive(:exec_params).and_return(mock_result)
        allow(mock_result).to receive(:cmd_tuples).and_return(1)

        expect { deleter.process_message(message) }.not_to raise_error
      end
    end

    context 'with people collection message' do
      let(:message) { { 'collection' => 'people', 'resource_id' => '123' } }

      it 'handles people deletion with cascades' do
        allow(mock_connection).to receive(:transaction).and_yield(mock_connection)

        # Mock person lookup
        person_result = instance_double(PG::Result)
        allow(person_result).to receive(:ntuples).and_return(1)
        allow(person_result).to receive(:[]).with(0).and_return({ 'id' => '456' })

        # Mock deletion results
        enrollment_result = instance_double(PG::Result)
        person_delete_result = instance_double(PG::Result)
        allow(enrollment_result).to receive(:cmd_tuples).and_return(2)
        allow(person_delete_result).to receive(:cmd_tuples).and_return(1)

        # Mock medical lookup for person
        medicals_result = instance_double(PG::Result)
        allow(medicals_result).to receive(:ntuples).and_return(0) # No medicals for simplicity
        allow(medicals_result).to receive(:[]).with(anything).and_return({ 'id' => '789', 'mongo_id' => 'abc' })
        allow(medicals_result).to receive(:each).and_return([]) # Support iteration for empty result

        expect(mock_connection).to receive(:exec_params)
          .with("SELECT id FROM dim_person WHERE mongo_id = $1", ['123'])
          .and_return(person_result)

        expect(mock_connection).to receive(:exec_params)
          .with("DELETE FROM fact_people_enrollment WHERE dim_person_fk = $1", ['456'])
          .and_return(enrollment_result)

        expect(mock_connection).to receive(:exec_params)
          .with("SELECT id, mongo_id FROM fact_medical WHERE dim_person_fk = $1", ['456'])
          .and_return(medicals_result)

        expect(mock_connection).to receive(:exec_params)
          .with("DELETE FROM dim_person WHERE id = $1", ['456'])
          .and_return(person_delete_result)

        expect { deleter.process_message(message) }.not_to raise_error
      end
    end

    context 'with medicals collection message' do
      let(:message) { { 'collection' => 'medicals', 'resource_id' => '123' } }

      it 'handles medical deletion with cascades' do
        allow(mock_connection).to receive(:transaction).and_yield(mock_connection)

        # Mock medical lookup
        medical_result = instance_double(PG::Result)
        allow(medical_result).to receive(:ntuples).and_return(1)
        allow(medical_result).to receive(:[]).with(0).and_return({ 'id' => '789' })

        # Mock deletion results
        health_category_result = instance_double(PG::Result)
        observation_result = instance_double(PG::Result)
        medical_delete_result = instance_double(PG::Result)

        allow(health_category_result).to receive(:cmd_tuples).and_return(1)
        allow(observation_result).to receive(:cmd_tuples).and_return(3)
        allow(medical_delete_result).to receive(:cmd_tuples).and_return(1)

        expect(mock_connection).to receive(:exec_params)
          .with("SELECT id FROM fact_medical WHERE mongo_id = $1", ['123'])
          .and_return(medical_result)

        expect(mock_connection).to receive(:exec_params)
          .with("DELETE FROM fact_health_category_report WHERE fact_medical_fk = $1", ['789'])
          .and_return(health_category_result)

        expect(mock_connection).to receive(:exec_params)
          .with("DELETE FROM fact_observation WHERE fact_medical_fk = $1", ['789'])
          .and_return(observation_result)

        expect(mock_connection).to receive(:exec_params)
          .with("DELETE FROM fact_medical WHERE id = $1", ['789'])
          .and_return(medical_delete_result)

        expect { deleter.process_message(message) }.not_to raise_error
      end
    end

    context 'with invalid message' do
      it 'raises error for missing collection' do
        message = { 'resource_id' => '123' }
        expect { deleter.process_message(message) }.to raise_error(ArgumentError, 'Missing collection')
      end

      it 'raises error for missing resource_id' do
        message = { 'collection' => 'appointments' }
        expect { deleter.process_message(message) }.to raise_error(ArgumentError, 'Missing resource_id')
      end

      it 'raises error for invalid collection' do
        message = { 'collection' => 'invalid', 'resource_id' => '123' }
        expect { deleter.process_message(message) }.to raise_error(ArgumentError, 'Invalid collection: invalid')
      end
    end
  end

  describe 'COLLECTION_TABLE_MAPPING' do
    it 'contains all expected mappings' do
      expected_mappings = {
        'appointments' => 'fact_appointment',
        'companies' => 'dim_organization',
        'contracts' => 'dim_contract',
        'groups' => 'dim_group',
        'medicals' => 'fact_medical',
        'locations' => 'dim_location',
        'people' => 'dim_person',
        'organisations' => 'dim_organization',
        'products' => 'dim_product',
        'programmes' => 'dim_programme'
      }

      expect(ResourceDeleter::COLLECTION_TABLE_MAPPING).to eq(expected_mappings)
    end
  end
end

RSpec.describe 'lambda_handler' do
  let(:context) { double('context') }
  let(:deleter) { double('ResourceDeleter') }

  before do
    allow(ResourceDeleter).to receive(:new).and_return(deleter)
    allow(deleter).to receive(:close_connection).and_return(nil)
    allow(deleter).to receive(:instance_variable_get).with(:@logger).and_return(double('logger', error: nil))
  end

  context 'with direct invocation' do
    let(:event) { { 'collection' => 'appointments', 'resource_id' => '123' } }

    it 'returns success response' do
      allow(deleter).to receive(:process_message)

      response = lambda_handler(event: event, context: context)

      expect(response[:statusCode]).to eq(200)
      expect(JSON.parse(response[:body])).to include(
        'message' => 'Successfully processed deletion',
        'collection' => 'appointments',
        'resource_id' => '123'
      )
    end
  end

  context 'with SQS message' do
    let(:sqs_body) { { 'collection' => 'people', 'resource_id' => '456' } }
    let(:event) do
      {
        'Records' => [{
          'body' => JSON.generate(sqs_body)
        }]
      }
    end

    it 'processes SQS message successfully' do
      allow(deleter).to receive(:process_message).with(sqs_body)

      response = lambda_handler(event: event, context: context)

      expect(response[:statusCode]).to eq(200)
      expect(JSON.parse(response[:body])).to include(
        'message' => 'Successfully processed deletion',
        'collection' => 'people',
        'resource_id' => '456'
      )
    end
  end

  context 'when error occurs' do
    let(:event) { { 'collection' => 'appointments', 'resource_id' => '123' } }
    let(:error) { StandardError.new('Database connection failed') }

    it 'returns error response' do
      allow(deleter).to receive(:process_message).and_raise(error)

      response = lambda_handler(event: event, context: context)

      expect(response[:statusCode]).to eq(500)
      expect(JSON.parse(response[:body])).to include(
        'error' => 'Database connection failed',
        'type' => 'StandardError'
      )
    end
  end
end
