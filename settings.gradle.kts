rootProject.name = "kafkademo"
include("kafka-basics")
include("kafka-producer-wikimedia")
include("kafka-consumer-opensearch")
include("kafka-consumer-opensearch:docker-compose.yml")
findProject(":kafka-consumer-opensearch:docker-compose.yml")?.name = "docker-compose.yml"
