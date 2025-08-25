package org.khanjabir909.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the StreamsBuilder instance
        StreamsBuilder builder = new StreamsBuilder();

        //Stream from Kafka
        KStream wordCountInput = builder.stream("word-count-input");

        // Process the stream
        // Convert all words to lowercase -
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> ((String) value).toLowerCase())
                //split the value into individual words and then apply flatMapValues
                .flatMapValues(value -> Arrays.asList(((String) value).split("\\W+")))
                //now map the copy the value to key as well
                .selectKey((key, value) -> value)
                //now we can group by key
                .groupByKey() //it returns GroupedKStream
                //count the occurrences of each word
                .count();  //it returns KTable<String, Long>
        // Write the results to a new topic
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        // Build the topology
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        // Start the streams application
        streams.start();

        //print the topology
        System.out.println(streams.toString());

        // Add a shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing the streams application...");
            streams.close();
        }));
    }
}
