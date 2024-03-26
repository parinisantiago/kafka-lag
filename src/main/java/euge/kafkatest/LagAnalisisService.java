package euge.kafkatest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class LagAnalisisService {

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private KafkaConsumer kafkaConsumer;


    @Scheduled(fixedDelay = 5000L)
    public void liveLagAnalysis() throws ExecutionException, InterruptedException {
        analyzeLag("lag-group");
    }

    public void analyzeLag(String groupId) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Long> consumerGrpOffsets = getConsumerGrpOffsets(groupId);
        Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerGrpOffsets);
        Map<TopicPartition, Long> lags = computeLags(consumerGrpOffsets, producerOffsets);
        for (Map.Entry<TopicPartition, Long> lagEntry : lags.entrySet()) {
            Long lag = lagEntry.getValue();
            System.out.println("Este es el lag: " + lag);
        }
    }

    private Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId) throws ExecutionException, InterruptedException {

        ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = info.partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, Long> groupOffset = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndMetadata metadata = entry.getValue();
            groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
        }
        return groupOffset;
    }

    private Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {

        List<TopicPartition> topicPartitions = new LinkedList<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
            TopicPartition key = entry.getKey();
            topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
        }
        return kafkaConsumer.endOffsets(topicPartitions);
    }

    private Map<TopicPartition, Long> computeLags(Map<TopicPartition, Long> consumerGrpOffsets, Map<TopicPartition, Long> producerOffsets) {
        Map<TopicPartition, Long> lags = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(producerOffset - consumerOffset);
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
    }


}
