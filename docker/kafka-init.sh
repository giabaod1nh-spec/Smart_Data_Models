#!/bin/bash
# Idempotent Kafka topic create + verify (K-1).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
RETRIES="${KAFKA_INIT_RETRIES:-60}"

echo "kafka-init: waiting for broker at ${BOOTSTRAP}..."
for i in $(seq 1 "${RETRIES}"); do
  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    echo "kafka-init: broker ready"
    break
  fi
  if [ "${i}" -eq "${RETRIES}" ]; then
    echo "kafka-init: broker not ready after ${RETRIES} attempts" >&2
    exit 1
  fi
  sleep 2
done

create_or_verify() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"
  local retention_bytes="$4"
  local segment_bytes="$5"

  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list | grep -qx "${topic}"; then
    echo "kafka-init: topic ${topic} exists — verifying"
    desc="$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}")"
    echo "${desc}"
    echo "${desc}" | grep -q "PartitionCount: ${partitions}" || {
      echo "kafka-init: FAIL ${topic} partition count mismatch (want ${partitions})" >&2
      exit 1
    }
    echo "${desc}" | grep -q "ReplicationFactor: 1" || {
      echo "kafka-init: FAIL ${topic} replication factor != 1" >&2
      exit 1
    }
  else
    echo "kafka-init: creating ${topic}"
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" \
      --create --topic "${topic}" \
      --partitions "${partitions}" \
      --replication-factor 1 \
      --config "cleanup.policy=delete" \
      --config "retention.ms=${retention_ms}" \
      --config "retention.bytes=${retention_bytes}" \
      --config "segment.bytes=${segment_bytes}"
  fi
}

RET_MS=604800000
RET_BYTES=1073741824
SEG_BYTES=134217728

create_or_verify "traffic.entity-events.v2" 3 "${RET_MS}" "${RET_BYTES}" "${SEG_BYTES}"
create_or_verify "traffic.entity-events.quarantine.v2" 1 "${RET_MS}" "${RET_BYTES}" "${SEG_BYTES}"
create_or_verify "traffic.entity-events.dlq.v2" 1 "${RET_MS}" "${RET_BYTES}" "${SEG_BYTES}"

echo "kafka-init: topics OK"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
