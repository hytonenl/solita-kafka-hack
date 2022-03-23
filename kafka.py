# Databricks notebook source
# MAGIC %md ## Define variables

# COMMAND ----------

# MAGIC %md
# MAGIC * From you Aiven Kafka cluster enable kafka_authentication_method.sasl for SASL authentication
# MAGIC * Fill in you CA Certificate, secrets and connection details as well as you topic you want to read

# COMMAND ----------

cacert = """
<cert>
"""
aiven_kafka_service_uri = ""
aiven_sasl_username = ""
aiven_sasl_password = ""
topic = ""

# COMMAND ----------

dbutils.fs.put("/databricks/init-scripts/custom-cert.sh", f"""#!/bin/bash

cat << 'EOF' > /usr/local/share/ca-certificates/myca.crt
{cacert}
EOF

update-ca-certificates

PEM_FILE="/etc/ssl/certs/myca.pem"
PASSWORD="changeit"
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
KEYSTORE="$JAVA_HOME/lib/security/cacerts"

CERTS=$(grep 'END CERTIFICATE' $PEM_FILE| wc -l)

# To process multiple certs with keytool, you need to extract
# each one from the PEM file and import it into the Java KeyStore.

for N in $(seq 0 $(($CERTS - 1))); do
  ALIAS="$(basename $PEM_FILE)-$N"
  echo "Adding to keystore with alias:$ALIAS"
  cat $PEM_FILE |
    awk "n==$N {{ print }}; /END CERTIFICATE/ {{ n++ }}" |
    keytool -noprompt -import -trustcacerts \
            -alias $ALIAS -keystore $KEYSTORE -storepass $PASSWORD
done

echo "export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt" >> /databricks/spark/conf/spark-env.sh
""", True)

# COMMAND ----------

# MAGIC %md ##NOTE: add the script above to cluster init scripts & restart the cluster before proceeding

# COMMAND ----------

# MAGIC %md ##Define Kafka options

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": aiven_kafka_service_uri,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.ssl.keystore.location": "/usr/lib/jvm/zulu8-ca-amd64/jre/lib/security/cacerts",
    "kafka.ssl.keystore.password": "changeit",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{aiven_sasl_username}' password='{aiven_sasl_password}';",
    "kafka.ssl.endpoint.identification.algorithm": "",
    "failOnDataLoss": "false",
    "subscribe": topic,
    #"startingOffsets": "latest",
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": 1000
}

# COMMAND ----------

# MAGIC %md ##Define your transformation functions (specific to your data)

# COMMAND ----------

process_cols = ["topic", "partition", "offset"]
json_cols = ["ride_id", "point_idx", "latitude", "longitude", "timestamp", "meter_reading", "meter_increment", "ride_status", "passenger_count"]

def transform(df):
    res = (df
            .withColumn("payload", col("value").cast("string"))
            .withColumn("message_b64", get_json_object(col("payload"), "$.message"))
            .withColumn("message", unbase64(col("message_b64")).cast("string"))
           )
    for column in json_cols:
        res = res.withColumn(column, get_json_object(col("message"), f"$.{column}"))
        
    return res

def select(df):
    return df.select(json_cols + process_cols)

# COMMAND ----------

# MAGIC %md ##Stream!

# COMMAND ----------

from pyspark.sql.functions import unbase64, col, split, get_json_object

stream = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
         )

message = transform(stream)
message = select(message).where("ride_id is not null")

# COMMAND ----------

# MAGIC %md ##Create a SQL view from the streaming DataFrame

# COMMAND ----------

message.createOrReplaceTempView("taxi_rides")

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_trunc("second", timestamp), sum(passenger_count), count(ride_id) as num_rides
# MAGIC from taxi_rides
# MAGIC group by date_trunc("second", timestamp)
# MAGIC order by date_trunc("second", timestamp) asc
