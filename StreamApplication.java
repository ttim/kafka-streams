import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;

import java.util.Properties;

public class StreamApplication {

    private static String updateValue(String newValue, String oldValue) {

        String[] newList = newValue.split(",");
        StringBuilder sb = new StringBuilder();
        sb.append(newValue);
        sb.append(":");

        if (Long.valueOf(newList[0]) >= Long.valueOf(newList[1])) {
            if (oldValue != null) {
                String[] oldList = oldValue.split(":")[0].split(",");
                if (Long.valueOf(oldList[0]) < Long.valueOf(oldList[1])) {
                    sb.append("OK");
                }
            }
        } else {
            sb.append(newValue);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = args[0];
        String studentTopic = args[1];
        String classroomTopic = args[2];
        String outputTopic = args[3];

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamApplication-yannick");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> studentStream = builder.stream(studentTopic);
        KStream<String, String> classroomStream = builder.stream(classroomTopic);

        KTable<String, String> studentTable = studentStream
                .filter((key, value) -> {
                    String[] tokens = value.split(",", -1);
                    return tokens.length == 2 && tokens[0].length() > 0 && tokens[1].length() > 0;
                })
                .map((key, value) -> {
                    String[] tokens = value.split(",", -1);
                    return KeyValue.pair(tokens[0], tokens[1]);
                })
                .groupByKey()
                .aggregate(
                        () -> null,
                        (key, newValue, aggValue) -> newValue
                );

        KTable<String, Long> classroomOccupancy = studentTable
                .groupBy((key, value) -> KeyValue.pair(value, 1L), Serialized.with(Serdes.String(), Serdes.Long()))
                .aggregate(
                        () -> 0L,
                        (key, newValue, aggValue) -> aggValue + newValue,
                        (key, oldValue, aggValue) -> aggValue - oldValue,
                        Materialized.with(Serdes.String(), Serdes.Long())
                );

        KTable<String, Long> classroomCapacity = classroomStream
                .filter((key, value) -> {
                    String[] tokens = value.split(",", -1);
                    if (tokens.length != 2 || tokens[0].length() == 0 || tokens[1].length() == 0) return false;
                    try {
                        long capacity = Long.valueOf(tokens[1]);
                        if (capacity < 0) return false;
                    } catch (Exception e) {
                        return false;
                    }
                    return true;
                })
                .map((key, value) -> {
                    String[] tokens = value.split(",");
                    return KeyValue.pair(tokens[0], Long.valueOf(tokens[1]));
                })
                .groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
                .aggregate(
                        () -> 0L,
                        (key, newValue, aggValue) -> newValue,
                        Materialized.with(Serdes.String(), Serdes.Long())
                );

        KTable<String, String> output = classroomCapacity.join(classroomOccupancy,
                (capacity, occupancy) -> capacity + "," + occupancy)
                .toStream().groupByKey()
                .aggregate(
                        () -> null,
                        (key, newValue, oldValue) -> updateValue(newValue, oldValue)
                );

        output.toStream()
                .filter((key, value) -> value.split(":").length == 2)
                .map((key, value) -> KeyValue.pair(null, key + "," + value.split(":")[1]))
                .to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
