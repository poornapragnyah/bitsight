if [ ! -d logs ]; then
    mkdir logs
fi


python spark/spark_streaming.py > logs/spark_streaming.log &
python kafka/rt_stock_producer.py > logs/rt_stock_producer.log &
python kafka/results_consumer.py > logs/results_consumer.log &
python kafka/persist_db_consumer.py > logs/persist_db_consumer.log