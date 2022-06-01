trap "exit" INT TERM ERR
trap "kill 0" EXIT

# Start Zookeeper
zkServer.sh start

# Start 3 Kafka Brokers
kafka-server-start.sh config/server0.properties &
kafka-server-start.sh config/server1.properties &
kafka-server-start.sh config/server2.properties &

wait
