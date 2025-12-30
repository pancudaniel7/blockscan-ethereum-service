#!/usr/bin/env bash
set -euo pipefail

BIN_DIR="/opt/kafka/bin"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
TOPIC="${KAFKA_TOPIC_NAME:-block-upstream-topic}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
TOPIC_CLEANUP_POLICY="${KAFKA_TOPIC_CLEANUP_POLICY:-compact}"
TOPIC_MAX_MSG_BYTES="${KAFKA_TOPIC_MAX_MESSAGE_BYTES:-}"
RETRIES="${KAFKA_PROVISIONER_RETRIES:-60}"
SLEEP_SECONDS="${KAFKA_PROVISIONER_SLEEP_SECONDS:-2}"

wait_for_kafka() {
	for ((attempt = 1; attempt <= RETRIES; attempt++)); do
		if "${BIN_DIR}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
			echo "Kafka is reachable after ${attempt} attempt(s)."
			return 0
		fi

		echo "Kafka not reachable yet (attempt ${attempt}/${RETRIES}), sleeping ${SLEEP_SECONDS}s..."
		sleep "${SLEEP_SECONDS}"
	done

	echo "Kafka broker not reachable after ${RETRIES} attempts." >&2
	return 1
}

topic_exists() {
	"${BIN_DIR}/kafka-topics.sh" \
		--bootstrap-server "${BOOTSTRAP}" \
		--describe \
		--topic "${TOPIC}" >/dev/null 2>&1
}

create_topic() {
    echo "Creating Kafka topic '${TOPIC}' (partitions=${PARTITIONS}, replication-factor=${REPLICATION})..."
    "${BIN_DIR}/kafka-topics.sh" \
        --bootstrap-server "${BOOTSTRAP}" \
        --create \
        --if-not-exists \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}" \
        --config "cleanup.policy=${TOPIC_CLEANUP_POLICY}" \
        $( [ -n "${TOPIC_MAX_MSG_BYTES}" ] && printf -- '--config max.message.bytes=%s' "${TOPIC_MAX_MSG_BYTES}" )
    echo "Kafka topic '${TOPIC}' ensured."
}

wait_for_kafka

if topic_exists; then
	echo "Kafka topic '${TOPIC}' already exists. Nothing to do."
else
	create_topic
fi
