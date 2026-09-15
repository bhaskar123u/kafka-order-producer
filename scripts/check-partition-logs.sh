check-partition-logs() {
    docker exec "$1" sh -c '
            find /tmp/kafka-logs -type f -path "/tmp/kafka-logs/order-events-*/*.log" |
            while read -r file; do
                name=${file#/tmp/kafka-logs/}

                echo
                echo "======================= $name ======================="
                /opt/kafka/bin/kafka-dump-log.sh \
                    --files "$file" \
                    --print-data-log
            done
        '
}