package kafkablocks.consumer;

interface KafkaListenerContainer {

    void start();

    void stop();

    void resume();

}
