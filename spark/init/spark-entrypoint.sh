#!/bin/bash
/usr/sbin/sshd

sleep 3

if [ "$SPARK_MODE" == "master" ]; then
    /opt/spark/sbin/start-master.sh
elif [ "$SPARK_MODE" == "worker" ]; then
    /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi

tail -f /opt/spark/logs/*
