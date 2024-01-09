TOPICS=("nifierror" "yfinance" "rss-ingestion-errors" "financial_windowed" "rss-parquets")
KAFKA_TOPICS="/usr/local/kafka/bin/kafka-topics.sh"
BOOTSTRAP_SERVER="node1:9092"


if ! command -v $KAFKA_TOPICS &> /dev/null ; then
  echo "${KAFKA_TOPICS} does not exist"
  exit 1
fi

existing_topics=$(${KAFKA_TOPICS} --bootstrap-server ${BOOTSTRAP_SERVER} --list)


for topic in "${TOPICS[@]}"; do
  if ${KAFKA_TOPICS} --bootstrap-server ${BOOTSTRAP_SERVER} --topic "${topic}" --describe &> /dev/null; then
    echo "topic ${topic} already exists!"
  else
    ${KAFKA_TOPICS} --bootstrap-server ${BOOTSTRAP_SERVER} --topic "${topic}" --create --partitions 1 --replication-factor 1
    echo "creating topic ${topic}"
  fi
done

for existing_topic in ${existing_topics[@]}; do
  if [[ ! " ${TOPICS[*]} " =~ ${existing_topic} ]]; then
    echo "deleting topic ${existing_topic}"
    ${KAFKA_TOPICS} --bootstrap-server ${BOOTSTRAP_SERVER} --topic "${existing_topic}" --delete
  fi
done

