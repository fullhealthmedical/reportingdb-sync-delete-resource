require 'pg'
require 'json'
require 'logger'

class ResourceDeleter
  COLLECTION_TABLE_MAPPING = {
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
  }.freeze

  VALID_COLLECTIONS = COLLECTION_TABLE_MAPPING.keys.freeze

  def initialize
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::INFO
  end

  def process_message(message)
    collection = message['collection']
    resource_id = message['resource_id']

    validate_message(collection, resource_id)

    @logger.info("Processing deletion for collection: #{collection}, resource_id: #{resource_id}")

    case collection
    when 'people'
      delete_person_with_cascades(resource_id)
    when 'medicals'
      delete_medical_with_cascades(resource_id)
    else
      delete_standard_record(collection, resource_id)
    end

    @logger.info("Successfully processed deletion for #{collection}/#{resource_id}")
  end

  private

  def validate_message(collection, resource_id)
    raise ArgumentError, 'Missing collection' unless collection
    raise ArgumentError, 'Missing resource_id' unless resource_id
    raise ArgumentError, "Invalid collection: #{collection}" unless VALID_COLLECTIONS.include?(collection)
  end

  def db_connection
    @db_connection ||= PG.connect(
      host: ENV['DB_HOST'],
      port: ENV['DB_PORT'] || 5432,
      dbname: ENV['DB_NAME'],
      user: ENV['DB_USER'],
      password: ENV['DB_PASSWORD']
    )
  end

  def delete_standard_record(collection, resource_id)
    table_name = COLLECTION_TABLE_MAPPING[collection]

    db_connection.transaction do |conn|
      result = conn.exec_params(
        "DELETE FROM #{table_name} WHERE mongo_id = $1",
        [resource_id]
      )

      @logger.info("Deleted #{result.cmd_tuples} record(s) from #{table_name}")

      if result.cmd_tuples.zero?
        @logger.warn("No records found to delete for #{collection}/#{resource_id}")
      end
    end
  end

  def delete_person_with_cascades(resource_id)
    db_connection.transaction do |conn|
      # First, get the person's internal ID
      person_result = conn.exec_params(
        'SELECT id FROM dim_person WHERE mongo_id = $1',
        [resource_id]
      )

      if person_result.ntuples.zero?
        @logger.warn("No person found with mongo_id: #{resource_id}")
        return
      end

      person_id = person_result[0]['id']
      @logger.info("Found person with id: #{person_id}")

      # Delete from fact_people_enrollment first (foreign key constraint)
      enrollment_result = conn.exec_params(
        'DELETE FROM fact_people_enrollment WHERE dim_person_fk = $1',
        [person_id]
      )
      @logger.info("Deleted #{enrollment_result.cmd_tuples} enrollment record(s)")

      # Find all medicals associated with this person
      medicals_result = conn.exec_params(
        'SELECT id, mongo_id FROM fact_medical WHERE dim_person_fk = $1',
        [person_id]
      )

      medicals_result.each do |medical_row|
        medical_id = medical_row['mongo_id']
        @logger.info("Deleting related medical record with id: #{medical_id}")
        delete_medical_with_cascades(medical_id)
      end

      # Delete the person record
      person_delete_result = conn.exec_params(
        'DELETE FROM dim_person WHERE id = $1',
        [person_id]
      )
      @logger.info("Deleted #{person_delete_result.cmd_tuples} person record(s)")
    end
  end

  def delete_medical_with_cascades(resource_id)
    db_connection.transaction do |conn|
      # First, get the medical record's internal ID
      medical_result = conn.exec_params(
        'SELECT id FROM fact_medical WHERE mongo_id = $1',
        [resource_id]
      )

      if medical_result.ntuples.zero?
        @logger.warn("No medical record found with mongo_id: #{resource_id}")
        return
      end

      medical_id = medical_result[0]['id']
      @logger.info("Found medical record with id: #{medical_id}")

      # Delete from fact_health_category_report first (foreign key constraint)
      health_category_result = conn.exec_params(
        'DELETE FROM fact_health_category_report WHERE fact_medical_fk = $1',
        [medical_id]
      )
      @logger.info("Deleted #{health_category_result.cmd_tuples} health category report record(s)")

      # Delete from fact_observation (foreign key constraint)
      observation_result = conn.exec_params(
        'DELETE FROM fact_observation WHERE fact_medical_fk = $1',
        [medical_id]
      )
      @logger.info("Deleted #{observation_result.cmd_tuples} observation record(s)")

      # Delete the medical record
      medical_delete_result = conn.exec_params(
        'DELETE FROM fact_medical WHERE id = $1',
        [medical_id]
      )
      @logger.info("Deleted #{medical_delete_result.cmd_tuples} medical record(s)")
    end
  end

  def close_connection
    @db_connection&.close
    @db_connection = nil
  end
end

def lambda_handler(event:, context:)
  deleter = ResourceDeleter.new

  begin
    # Handle both direct message format and SQS/SNS wrapped messages
    message = if event['Records']
                # SQS message format
                JSON.parse(event['Records'].first['body'])
              else
                # Direct invocation
                event
              end

    deleter.process_message(message)

    {
      statusCode: 200,
      body: JSON.generate({
        message: 'Successfully processed deletion',
        collection: message['collection'],
        resource_id: message['resource_id']
      })
    }
  rescue => e
    deleter.instance_variable_get(:@logger).error("Error processing message: #{e.message}")
    deleter.instance_variable_get(:@logger).error("Backtrace: #{e.backtrace.join("\n")}")

    {
      statusCode: 500,
      body: JSON.generate({
        error: e.message,
        type: e.class.name
      })
    }
  ensure
    deleter.close_connection
  end
end
