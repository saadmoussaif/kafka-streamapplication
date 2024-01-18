package ma.saadmoussaif.springbootkafkademoapp.web;


import ma.saadmoussaif.springbootkafkademoapp.entities.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {

    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private InteractiveQueryService interactiveQueryService; // exchange with store

    @GetMapping("/pubish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String name) {
        PageEvent pageEvent = new PageEvent();
        pageEvent.setName(name);
        pageEvent.setDate(new Date());
        pageEvent.setDuration(new Random().nextInt(1000));
        pageEvent.setUser(Math.random() > 0.5 ? "U1" : "U2");
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1)) // generate new strem in 1s
                .map(sequence -> {
                    Map<String, Long> stringLongMap = new HashMap<>();
                    // call store that we created for that we call interactiveQueryService
                    ReadOnlyWindowStore<String, Long> windowStore = // type  de store window
                            interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());

                    Instant now = Instant.now(); // now
                    Instant from = now.minusMillis(5000); // before 5s,

                    KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now);
                     /*
                        if we just one a s specifique key
                         KeyValueIterator<Windowed<String>, Long> fetchAll windowStore.fetch("P1", from, now);
                      */
                    while (fetchAll.hasNext()) {
                        KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        // generate a new data :
                        stringLongMap.put(next.key.key(), next.value);
                    }
                    return stringLongMap;
                }).share();
    }





}
