package io.kafbat.ui.service;

import io.kafbat.ui.emitter.EnhancedConsumer;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.serdes.builtin.mm2.OffsetSyncSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class Mm2LagService {
    private static final OffsetSyncSerde OFFSET_SYNC_SERDE = new OffsetSyncSerde();

    private final AdminClientService adminClientService;
    private final ClustersStorage clustersStorage;
    private final ConsumerGroupService consumerGroupService;

  public Mono<Map<String, Long>> computeLagForCluster(KafkaCluster targetCluster) {
    return adminClientService.get(targetCluster)
        .flatMap(ac -> ac.topics(true))
        .flatMap(allTopics -> {
          // Find all mm2-offset-syncs.*.internal topics on this cluster
          List<String> syncTopics = allTopics.stream()
              .filter(t -> OffsetSyncSerde.TOPIC_NAME_PATTERN.matcher(t).matches())
              .collect(Collectors.toList());

          if (syncTopics.isEmpty()) {
            log.debug("No MM2 offset-sync topics found on cluster {}", targetCluster.getName());
            return Mono.just(Collections.<String, Long>emptyMap());
          }

          log.debug("Found MM2 offset-sync topics on {}: {}", targetCluster.getName(), syncTopics);

          return Mono.fromCallable(() -> computeLagFromSyncTopics(targetCluster, syncTopics))
              .subscribeOn(Schedulers.boundedElastic());
        })
        .onErrorResume(e -> {
          log.warn("Failed to compute MM2 lag for cluster {}: {}", targetCluster.getName(), e.getMessage());
          return Mono.just(Collections.emptyMap());
        });
  }

    private Map<String, Long> computeLagFromSyncTopics(KafkaCluster targetCluster, List<String> syncTopics){

      Map<String, Map<Integer, Long>> syncedOffsets =new HashMap<>();

      for(String syncTopic: syncTopics){
        try(EnhancedConsumer consumer =consumerGroupService.createConsumer(targetCluster)){
          Map<String, Map<Integer, Long>> topicSyncs = readOffsetSyncsTopic(consumer,syncTopic);

          topicSyncs.forEach((topic,partMap)->
              syncedOffsets.merge(topic,partMap,(existing,incoming)->{
                Map<Integer,Long> merged= new HashMap<>(existing);
                incoming.forEach((part, off)-> merged.merge(part,off, Math::max));
                return merged;
              })
          );
        } catch (Exception e){
          log.error("Error reading offset-sync topic {} on cluster {}", syncTopic,
              targetCluster.getName(),e);
        }
      }
      if(syncedOffsets.isEmpty()){
        return Collections.emptyMap();
      }

      return fetchLagWithSourceLeo(targetCluster,syncedOffsets);
    }

    private Map<String, Long> fetchLagWithSourceLeo(KafkaCluster targetCluster,Map<String, Map<Integer, Long>> syncedOffsets ){
      KafkaCluster sourceCluster =findSourceCluster(targetCluster,syncedOffsets.keySet());
      if(sourceCluster == null){
        log.debug("Could not find source cluster for MM2 lag on {}, "
         + "Falling back to target LEO comparison.", targetCluster.getName());
        return computeLagUsingTargetLeo(targetCluster,syncedOffsets);
      }
      return computeLagUsingSourceLeo(sourceCluster,syncedOffsets);
    }

    private KafkaCluster findSourceCluster(KafkaCluster targetCluster, Set<String> syncedTopics){
      for (KafkaCluster candidate : clustersStorage.getKafkaClusters()){
        if(candidate.getName().equals(targetCluster.getName())) continue;
        try{
          Set<String> candidateTopics =adminClientService.get(candidate)
              .flatMap(ac -> ac.listTopics(false))
              .block(Duration.ofSeconds(5));
          if (candidateTopics !=null && !Collections.disjoint(candidateTopics,syncedTopics)){
            log.debug("Identified source cluster {} for MM2 Lag on {}",
            candidate.getName(), targetCluster.getName());
            return candidate;
          }
        }catch (Exception e){
          log.debug("Could not check cluster {} as MM2 Source: {}", candidate.getName(), e.getMessage());
        }
      }
      return null;
    }

  private Map<String, Long> computeLagUsingSourceLeo(KafkaCluster sourceCluster, Map<String, Map<Integer, Long>> syncedOffsets) {
    Map<String, Long> result = new HashMap<>();
    try {
      List<TopicPartition> tps = new ArrayList<>();
      syncedOffsets.forEach((topic, partMap) ->
          partMap.keySet().forEach(part -> tps.add(new TopicPartition(topic, part))
          ));
      Map<TopicPartition, Long> sourceLeos = adminClientService.get(sourceCluster)
          .flatMap(ac -> ac.listOffsets(tps, org.apache.kafka.clients.admin.OffsetSpec.latest(), false))
          .block(Duration.ofSeconds(10));
      if (sourceLeos == null) return Collections.emptyMap();

      syncedOffsets.forEach((topic, partMap) -> {
        long topicLag = 0;
        for (Map.Entry<Integer, Long> e : partMap.entrySet()) {
          TopicPartition tp = new TopicPartition(topic, e.getKey());
          Long sourceLeo = sourceLeos.get(tp);
          if (sourceLeo != null) {
            topicLag += Math.max(0, sourceLeo - (e.getValue() + 1));
          }
        }
        result.put(topic, topicLag);
      });
    } catch (Exception e) {
      log.warn("Error fetching source LEOs for MM2 lag: {}", e.getMessage());
    }
    return result;
  }

  private Map<String, Long> computeLagUsingTargetLeo(KafkaCluster targetCluster,Map<String, Map<Integer, Long>> syncedOffsets) {
    Map<String, Long> result = new HashMap<>();
    try {
      List<TopicPartition> tps = new ArrayList<>();
      syncedOffsets.forEach((topic, partMap) ->
          partMap.keySet().forEach(part -> tps.add(new TopicPartition(topic, part)))
      );

      Map<TopicPartition, Long> targetLeos = adminClientService.get(targetCluster)
          .flatMap(ac -> ac.listOffsets(tps, org.apache.kafka.clients.admin.OffsetSpec.latest(), false))
          .block(Duration.ofSeconds(10));

      if (targetLeos == null) return Collections.emptyMap();

      syncedOffsets.forEach((topic, partMap) -> {
        long topicLag = 0;
        for (Map.Entry<Integer, Long> e : partMap.entrySet()) {
          TopicPartition tp = new TopicPartition(topic, e.getKey());
          Long targetLeo = targetLeos.get(tp);
          if (targetLeo != null) {
            topicLag += Math.max(0, targetLeo - (e.getValue() + 1));
          }
        }
        result.put(topic, topicLag);
      });
    } catch (Exception e) {
      log.warn("Error fetching target LEOs for MM2 lag fallback: {}", e.getMessage());
    }
    return result;
  }

  private Map<String, Map<Integer, Long>> readOffsetSyncsTopic(EnhancedConsumer consumer,String syncTopic) {
    Map<String, Map<Integer, Long>> result = new HashMap<>();
    try {
      List<TopicPartition> partitions = consumer.partitionsFor(syncTopic).stream()
          .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
          .collect(Collectors.toList());

      if (partitions.isEmpty()) return result;

      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);

      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
      long totalRecordsToRead = endOffsets.values().stream().mapToLong(Long::longValue).sum();

      if (totalRecordsToRead == 0) return result;

      long recordsRead = 0;
      while (recordsRead < totalRecordsToRead) {
        ConsumerRecords<Bytes, Bytes> records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) break;

        for (ConsumerRecord<Bytes, Bytes> record : records) {
          recordsRead++;
          if (record.key() == null || record.value() == null) continue;
          try {
            Struct key = OFFSET_SYNC_SERDE.deserializeKey(record.key().get());
            Struct value = OFFSET_SYNC_SERDE.deserializeValue(record.value().get());

            String sourceTopic = (String) key.get("topic");
            int partition = (int) key.get("partition");
            long upstreamOffset = (long) value.get("upstreamOffset");

            result.computeIfAbsent(sourceTopic, k -> new HashMap<>())
                .merge(partition, upstreamOffset, Math::max);
          } catch (Exception e) {
            log.trace("Could not decode offset-sync record from {}: {}", syncTopic, e.getMessage());
          }
        }
      }

      log.debug("Read {} records from {} on cluster, found {} source topics",
          recordsRead, syncTopic, result.size());
    } catch (Exception e) {
      log.warn("Error reading offset-sync topic {}: {}", syncTopic, e.getMessage());
    }

    return result;
  }
}
