# Bitcoin Analysis

source: https://github.com/RohanJnr/bitcoin-analysis-spark-kafka

## Project Requirements
1. Python
2. Docker
3. Docker Compose
4. PgAdmin (not compulsary, can use psql shell)


## Project Setup
1. Run `docker compose up` to start all docker containers
2. Create python virtualenv and install packages from requirements.txt
```
python -m venv venv
# On Unix
source /venv/bin/activate
pip install -r requirements.txt
```

## Streaming Process
1. Run the bash file `streaming.sh`
```
chmod +x streaming.sh
./streaming.sh
```
2. View logs in the `logs/spark_streaming.log` file.
3. Verify database is being popular by using pgadmin or psql inside the postgres container.
```
# using psql inside container
docker exec -it <container hash> bash
psql -U <username> -d <database>
```


## Batch process
0. Correct the postgres JDBC jar file path in line 8 of `spark/spark_batch.py`
1. Once the database is populated to a certain extent, batch processing can be run.

```
python spark/spark_batch.py
```