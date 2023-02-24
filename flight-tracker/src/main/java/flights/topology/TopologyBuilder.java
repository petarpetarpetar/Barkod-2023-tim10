package flights.topology;

import flights.serde.Serde;
import java.util.Properties;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import radar.AirportUpdateEvent;
import radar.FlightUpdateEvent;

public class TopologyBuilder implements Serde {

    private Properties config;

    public TopologyBuilder(Properties properties) {
        this.config = properties;
    }

    private static final Logger logger = LogManager.getLogger(TopologyBuilder.class);

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        String schemaRegistry = config.getProperty("kafka.schema.registry.url");

        KStream<String, FlightUpdateEvent> flightInputStream = builder.stream(
                config.getProperty("kafka.topic.flight.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(FlightUpdateEvent.class, schemaRegistry)));

        GlobalKTable<String, AirportUpdateEvent> airportTable = builder.globalTable(
                config.getProperty("kafka.topic.airport.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(AirportUpdateEvent.class, schemaRegistry)));

        return builder.build();
    }
}
