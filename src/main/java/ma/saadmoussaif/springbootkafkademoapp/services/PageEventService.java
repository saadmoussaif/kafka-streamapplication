package ma.saadmoussaif.springbootkafkademoapp.services;


import ma.saadmoussaif.springbootkafkademoapp.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


@Service
public class PageEventService {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input) -> {
            try {
                System.out.println("-**-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-");
                System.out.println(input.toString());
                System.out.println("-**-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-");
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

     /*
       Suplier each time send data to specifique service !!
     */

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> {
            PageEvent pageEvent = new PageEvent();
            pageEvent.setName(Math.random() > 0.5 ? "P1" : "P2");
            pageEvent.setDate(new Date());
            pageEvent.setDuration(new Random().nextInt(1000));
            pageEvent.setUser(Math.random() > 0.5 ? "U1" : "U2");
            return pageEvent;
        };
    }


    /*
        take data from source and send it to an other source (Topic)
     */
    @Bean
    public Function<PageEvent, PageEvent> pageEventFunction() {
        return (input) -> {
            input.setName("Page Event ");
            input.setUser("UUUU");
            return input;
        };
    }


    @Bean
    public  Function<KStream<String,PageEvent>,KStream<String,Long>> kStreamKStreamFunction() {
        return (input) -> {
            return input
                    .filter((k, v) -> v.getDuration() > 100)
                    .map((k, v) -> new KeyValue<>(v.getName(), 0L))
                    .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))

                    /*
                       windowed By here we calculate just the evolution in period of time !
                     */
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                    /*
                        create a store called page-count
                     */
                    .count(Materialized.as("page-count"))
                    .toStream()
                    .map((k, v) -> new KeyValue<>(k.key(), v));

        };
    }

//    @Bean
//    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamKStreamFunction() {
//        return input -> {
//            return input
//                    .filter((k, v) -> v.getDuration() > 100)
//                    .map((k, v) -> new KeyValue<>(v.getName(), 1L))
//                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())) // Group by key
//                    .count() // Count occurrences
//                    .toStream();
//        };
//    }

}
