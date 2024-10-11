plugins {
    id("java")
}

group = "net.demo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    // https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
    implementation ("com.fasterxml.jackson.core:jackson-core:2.17.2")

    // https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
    implementation ("com.fasterxml.jackson.core:jackson-databind:2.17.2")

    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
    implementation ("org.apache.kafka:kafka-streams:3.6.1")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.16")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}