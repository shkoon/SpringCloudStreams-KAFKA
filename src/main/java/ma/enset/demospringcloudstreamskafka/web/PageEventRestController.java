package ma.enset.demospringcloudstreamskafka.web;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import ma.enset.demospringcloudstreamskafka.entities.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
@RequestMapping("api/page-event")

public class PageEventRestController {
    @Autowired
    private StreamBridge streamBridge;
    @Autowired
    private InteractiveQueryService interactiveQueryService;
    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable(name = "topic") String topic,
                             @PathVariable(name = "name") String name){
        PageEvent pageEvent=PageEvent.builder().name(name).user(Math.random()>0.5?"U2":"U1")
                .duration(new Random().nextInt(9000)).vistedAt(new Date()).build();
        System.out.println(topic);
        streamBridge.send(topic,pageEvent);

        return pageEvent;
    }

    @GetMapping(path = "/analytics",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(1)).map(sequence->{
            Map<String,Long > stringLongMap=new HashMap<>();
            ReadOnlyWindowStore<String,Long> stats=interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
            Instant instant=Instant.now();
            Instant from=instant.minusMillis(5000);
            KeyValueIterator<Windowed<String>,Long> fetchAll=stats.fetchAll(from,instant);
            while (fetchAll.hasNext()){
                KeyValue<Windowed<String>,Long> next=fetchAll.next();
                stringLongMap.put(next.key.key(),next.value);
            }
            return  stringLongMap;
        }).share();
    }


}