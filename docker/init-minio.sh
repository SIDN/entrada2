#!/bin/sh

/usr/bin/mc config host add myminio http://minio:9000 admin $DEFAULT_PASSWORD;
/usr/bin/mc admin user add myminio entrada $DEFAULT_PASSWORD;
/usr/bin/mc admin policy attach myminio readwrite --user entrada;
/usr/bin/mc config host add entrada http://minio:9000 entrada $DEFAULT_PASSWORD;
/usr/bin/mc mb --ignore-existing --region eu-west-1 entrada/$ENTRADA_S3_BUCKET;
/usr/bin/mc event add entrada/$ENTRADA_S3_BUCKET arn:minio:sqs::ENTRADA:amqp -p --event put --prefix pcap-in/;