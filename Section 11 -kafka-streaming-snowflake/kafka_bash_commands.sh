# Kafka steps 

Step-1 - bin/zookeeper-server-start.sh config/zookeeper.properties
Step-2 - bin/kafka-server-start.sh config/server.properties
Step-3 - bin/kafka-topics.sh --create --topic sales-data --bootstrap-server localhost:9092

#  Delete a topic 
 - bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test-data

 #  list all topics 
 - bin/kafka-topics.sh --list --bootstrap-server localhost:9092

Step-4 - bin/kafka-console-consumer.sh --topic sales-data --from-beginning --bootstrap-server localhost:9092

Step-6  bin/connect-standalone.sh config/connect-standalone.properties config/SF_connect.properties


# Create an unencrypted private key 
openssl genrsa -out rsa_key.pem 2048

# Create a public key referencing the above private key
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub