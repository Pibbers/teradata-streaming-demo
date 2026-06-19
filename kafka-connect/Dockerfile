FROM confluentinc/cp-kafka-connect:7.6.1

# Pre-install the S3 sink connector so the image works without internet at runtime
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.14
