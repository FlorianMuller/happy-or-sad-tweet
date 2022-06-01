trap "exit" INT TERM ERR
trap "kill 0" EXIT

zkServer.sh start


kafka-server-start.sh config/server0.properties &
kafka-server-start.sh config/server1.properties &
kafka-server-start.sh config/server2.properties &

wait
