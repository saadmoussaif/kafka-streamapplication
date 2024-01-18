package ma.saadmoussaif.springbootkafkademoapp.services;

import ma.saadmoussaif.springbootkafkademoapp.entities.CarPosition;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;


@Service
public class CarPositionSup{

    @Bean
    public Supplier<Map<String, CarPosition>> carPositionSupplier() {
        return () -> {
            Map<String, CarPosition> carPositions = new HashMap<>();
            carPositions.put("car1", generateRandomCarPosition());
            carPositions.put("car2", generateRandomCarPosition());
            // Add more cars as needed
            return carPositions;
        };
    }

    @Bean
    public Function<KStream<String, CarPosition>, KStream<String, Long>> kStreamKStreamFunctionCar() {
        return (input) -> {
            return input
                    .map((k, v) -> new KeyValue<>(k, 1L)) // Map each car to a key-value pair with value 1
                    .groupByKey() // Group by carId
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(3)))
                    .count(Materialized.as("cars-count")) // Count occurrences within the window
                    .toStream()
                    .map((key, value) -> new KeyValue<>(key.key(), value));
        };
    }



    private CarPosition generateRandomCarPosition() {
        Random random = new Random();
        double latitude = 30 + (40 - 30) * random.nextDouble(); // Example latitude range: 30 to 40
        double longitude = -5 + (5 - (-5)) * random.nextDouble(); // Example longitude range: -5 to 5
        return new CarPosition(latitude, longitude);
    }


}