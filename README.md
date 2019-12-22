# Kafka blocks

This is some useful modules for working with Apache Kafka.

It includes:
- KafkaPublisher - for publishing events to kafka
- KafkaConsumer - for consuming events
- EventProcessorRunner - for running Kafka Streams processors with automatic topology building

All these modules work in the paradigm:

**_One Kafka topic for one event class accurate to inheritance_**

## Examples

**Requirements**
* Docker & Docker Compose
* Maven
* Java 11 or higher; if necessary, set JAVA_HOME in JDK v11+ directory


    > set JAVA_HOME=C:\Program Files\Java\jdk-12.0.2\

**Building & running**

    > git clone https://github.com/a-konyaev/kafka-blocks.git
    > cd kafka-blocks
    > mvn clean install
    > cd ./examples/bin
    > up.bat
