#!/bin/bash
ordinal=$(echo $POD_NAME | rev | cut -d'-' -f1 | rev)
export KAFKA_BROKER_ID=$ordinal

exec /opt/bitnami/scripts/kafka/run.sh
