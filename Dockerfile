FROM public.ecr.aws/lambda/ruby:3.2

# Copy function code
COPY reportingdb_sync_delete_resource.rb ${LAMBDA_TASK_ROOT}

# Copy Gemfile and install dependencies
COPY Gemfile ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN bundle config set --local deployment 'true' && \
    bundle config set --local without 'development test' && \
    bundle install

# Set the CMD to your handler
CMD [ "lambda_function.lambda_handler" ]
