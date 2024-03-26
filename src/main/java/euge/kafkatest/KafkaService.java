package euge.kafkatest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.kafka.core.*;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class KafkaService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 1000L, initialDelay = 5L)
    public void sendMessage()  {
        String message = "msg-" + LocalDateTime.now();
        this.kafkaTemplate.send("lag-test", message);
    }

    //@KafkaListener(topics = "myTopic", containerFactory = "kafkaListenerContainerFactory", autoStartup = "${monitor.consumer.simulate}")
    public void testMetrics(Consumer<?, ?> cr) throws Exception {
        Map<MetricName, ? extends Metric> metrics = cr.metrics();
        System.out.println("---------------------------------------------");
        printMetric("lag-max", metrics);
        System.out.println("---------------------------------------------");
        Thread.sleep(2000L);
    }


    @KafkaListener(topics = "lag-test",containerFactory = "kafkaListenerContainerFactory",autoStartup = "${monitor.consumer.simulate}")
    public void testLag(String message) throws InterruptedException {
        System.out.println(message);
        Thread.sleep(2000L);
    }

    private static void printMetric(String tag, Map<MetricName, ? extends Metric> metrics) {
        MetricName metricName = metrics.keySet()
                                        .stream()
                                        .filter(m -> tag.equalsIgnoreCase(m.name()))
                                        .findFirst()
                                        .orElse(null);

        if(metricName == null) {
            System.out.println("La variable esta en null");
        } else {
            Metric metric = metrics.get(metricName);
            System.out.println("valor de la metrica "+ tag +": " + metric.metricValue());
        }
    }
}
