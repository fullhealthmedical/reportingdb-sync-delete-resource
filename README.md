# ResourceDeleter Lambda Function

This AWS Lambda function processes deletion messages for various collections and removes corresponding records from PostgreSQL database tables. It handles both simple deletions and cascading deletions for related records.

## Overview

The Lambda function receives messages in the following format:
```json
{
  "collection": "users",
  "resource_id": "123"
}
```

## Collection Mappings

The function supports the following collection to table mappings:

| Collection | PostgreSQL Table |
|------------|------------------|
| appointments | fact_appointment |
| companies | dim_organization |
| contracts | dim_contract |
| groups | dim_group |
| medicals | fact_medical |
| locations | dim_location |
| people | dim_person |
| organisations | dim_organization |
| products | dim_product |
| programmes | dim_programme |

## Special Cases

### People Collection
When processing `collection: "people"`, the function:
1. Finds the person record in `dim_person` using `mongo_id`
2. Deletes all related records in `fact_people_enrollment`
3. Deletes the person record from `dim_person`

### Medicals Collection
When processing `collection: "medicals"`, the function:
1. Finds the medical record in `fact_medical` using `mongo_id`
2. Deletes all related records in `fact_health_category_report`
3. Deletes all related records in `fact_observation`
4. Deletes the medical record from `fact_medical`

## Database Schema

### Person Tables
```sql
CREATE TABLE dim_person (
    id BIGSERIAL PRIMARY KEY,
    mongo_id VARCHAR(24) NOT NULL UNIQUE  
);

CREATE TABLE fact_people_enrollment (
    id BIGSERIAL PRIMARY KEY,
    dim_person_fk BIGINT NOT NULL REFERENCES dim_person(id)
);
```

### Medical Tables
```sql
CREATE TABLE fact_medical (
    id BIGSERIAL PRIMARY KEY,
    mongo_id VARCHAR(24) NOT NULL UNIQUE
);

CREATE TABLE fact_health_category_report (
    id BIGSERIAL PRIMARY KEY,
    fact_medical_fk BIGINT NOT NULL REFERENCES fact_medical(id)
);

CREATE TABLE fact_observation (
    id BIGSERIAL PRIMARY KEY,
    fact_medical_fk BIGINT NOT NULL REFERENCES fact_medical(id)
);
```

## Environment Variables

The Lambda function requires the following environment variables:

- `DB_HOST`: PostgreSQL database host
- `DB_PORT`: PostgreSQL database port (default: 5432)
- `DB_NAME`: PostgreSQL database name
- `DB_USER`: PostgreSQL database user
- `DB_PASSWORD`: PostgreSQL database password

## Development Setup

### Prerequisites
- Ruby 3.2.0
- Bundler
- Docker (for containerized deployment)
- PostgreSQL (for integration tests)

### Installation
```bash
bundle install
```

### Running Tests

#### Unit Tests
```bash
bundle exec rspec spec/lambda_function_spec.rb
```

#### Integration Tests
Integration tests require a PostgreSQL database. Set up the test database environment variables:

```bash
export TEST_DB_HOST=localhost
export TEST_DB_PORT=5432
export TEST_DB_NAME=test_reportingdb
export TEST_DB_USER=test_user
export TEST_DB_PASSWORD=test_password
export RUN_INTEGRATION_TESTS=true

bundle exec rspec --tag integration
```

#### All Tests
```bash
bundle exec rspec
```

## Deployment

This Lambda function is designed to be deployed via CI/CD pipeline. The `Dockerfile` is provided for containerized deployment to AWS Lambda.

### Docker Build

Build the container image:
```bash
docker build -t resource-deleter:latest .
```

### Local Testing

1. Run locally with Docker:
```bash
docker run -p 9000:8080 \
  -e DB_HOST=your-db-host \
  -e DB_PORT=5432 \
  -e DB_NAME=your-db-name \
  -e DB_USER=your-db-user \
  -e DB_PASSWORD=your-db-password \
  resource-deleter
```

2. Test locally:
```bash
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"collection": "appointments", "resource_id": "test123"}'
```

## Message Formats

### Direct Invocation
```json
{
  "collection": "people",
  "resource_id": "507f1f77bcf86cd799439011"
}
```

### SQS Message
```json
{
  "Records": [{
    "body": "{\"collection\": \"medicals\", \"resource_id\": \"507f1f77bcf86cd799439012\"}"
  }]
}
```

## Error Handling

The function includes comprehensive error handling:

- **Invalid collection**: Returns 500 error with validation message
- **Missing parameters**: Returns 500 error with validation message
- **Database connection errors**: Returns 500 error with connection details
- **Record not found**: Logs warning but completes successfully
- **Transaction rollback**: All database operations are wrapped in transactions

## Logging

The function logs the following information:
- Message processing start/completion
- Number of records deleted from each table
- Warnings when no records are found
- Error details including stack traces

## Monitoring

Key metrics to monitor:
- Lambda function duration
- Lambda function errors
- Database connection timeouts
- SQS message processing rate
- Dead letter queue message count

## CI/CD Deployment Notes

This function is designed for deployment via CI/CD pipeline. Consider the following:

- Use the provided `Dockerfile` for containerized Lambda deployment
- Environment variables should be managed through your deployment pipeline
- Database credentials should be stored in AWS Secrets Manager or similar
- Lambda function should run in a VPC with proper security groups
- Minimal database permissions (DELETE only on required tables)
- Network access limited to database subnets only
