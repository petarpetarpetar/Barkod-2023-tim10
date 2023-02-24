package flights.topology;

import flights.serde.Serde;
import java.util.Properties;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import radar.AirportUpdateEvent;
import radar.FlightUpdateEvent;
import radar.TransformedFlight;
import org.apache.kafka.streams.KeyValue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.kstream.Printed;

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


        KStream<String, TransformedFlight> output = flightInputStream.map((key, value) -> new KeyValue<>(key, tf(value)));

        output.to("radar.flights", Produced.with(Serde.stringSerde, Serde.specificSerde(TransformedFlight.class, schemaRegistry)));

        // TODO:
        // 2. ZADATAK: (NE RADI NAM TIME WINDOW) 
        // TimeWindow window = TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1));

        // KStream<String, TransformedFlight> output2;
        // output2 = flightInputStream.filter((key, value) -> (!value.getStatus().toString().equals("LATE")) && !value.getStatus().toString().equals("CANCELED"))
        //     .windowedBy(window)
        //     .peek((key,value) -> {System.out.println(value);});

        // output2.to("radar.airports.kpi", Produced.with(Serde.stringSerde, Serde.specificSerde(TransformedFlight.class, schemaRegistry)));        
        return builder.build();
    }

    public TransformedFlight tf(FlightUpdateEvent in){
        String to = in.getDestination().toString().split("->")[1];
        String from = in.getDestination().toString().split("->")[0];

        return new TransformedFlight(in.getId(), 
                                    in.getDate(),
                                    to,
                                    from,
                                    in.getSTD(),
                                    in.getSTA(),
                                    CalculateFlightDuration(in.getSTA(), in.getSTD()),
                                    convertToISO(in.getTimezones().toString(), 0, in.getSTD()),
                                    convertToISO(in.getTimezones().toString(), 1, in.getSTA()),
                                    extractAirportCode(in.getDestination().toString(),0),
                                    extractAirportCode(in.getDestination().toString(),1),
                                    in.getStatus().toString(),
                                    in.getGate().toString(),
                                    in.getAirline().toString()
        );
    }


    public long CalculateFlightDuration(long now, long then){
        long minutes = TimeUnit.MILLISECONDS.toMinutes(now - then);
        return minutes;
    }

    public String convertToISO(String timezones, int index, long timestamp){
        String tz = timezones.split("->")[index];

        TimeZone TIMEZONE = TimeZone.getTimeZone(tz);
        
        Date date = new Date(timestamp);
        // Conversion
        SimpleDateFormat sdf;
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TIMEZONE);
        String text = sdf.format(date);

        return text;
    }

    public String extractAirportCode(String destination, int index){
        String name = destination.split("->")[index];
        String code = StringUtils.substringBetween(name,"(", ")");
        return code;
    }
}