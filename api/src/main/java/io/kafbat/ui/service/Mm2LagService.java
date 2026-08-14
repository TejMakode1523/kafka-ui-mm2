package io.kafbat.ui.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.emitter.EnhancedConsumer;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.serdes.builtin.mm2.OffsetSyncSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Computes MM2 replication lag for each cluster (treated as TARGET).
 *
 * <h3>Multi-source support</h3>
 * <p>When multiple MM2 connectors mirror from different source clusters into the same
 * target, each connector creates its own pair of internal topics using its alias:
 * <pre>
 *   mm2-offset-syncs.{alias}.internal  <- upstreamOffsets per source topic/partition
 *   mm2-configs.{alias}.internal       <- full connector config incl. source bootstrap
 * </pre>
 *
 * <p>The alias is the link. This service:
 * <ol>
 *   <li>Finds all {@code mm2-offset-syncs.*.internal} topics on the target.</li>
 *   <li>For each sync topic, extracts the alias from the topic name.</li>
 *   <li>Reads the <em>matching</em> {@code mm2-configs.{alias}.internal} topic to get
 *       the source bootstrap servers for that specific connector.</li>
 *   <li>Creates a temporary AdminClient to that source and fetches LEOs.</li>
 *   <li>Computes lag = max(0, sourceLEO - (upstreamOffset + 1)) per partition.</li>
 *   <li>Merges results from all connectors into one map.</li>
 * </ol>
 *
 * <p>Falls back to target LEO comparison per connector if source is unreachable.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class Mm2LagService {

  private static final OffsetSyncSerde OFFSET_SYNC_SERDE = new OffsetSyncSerde();
  private static final ObjectMapper    MAPPER            = new ObjectMapper();

  // mm2-offset-syncs.{alias}.internal -- group 1 = alias
  private static final Pattern SYNC_TOPIC_PATTERN =
      Pattern.compile("mm2-offset-syncs\\.(.+)\\.internal");

  // mm2-configs.{alias}.internal
  private static final Pattern CONFIGS_TOPIC_PATTERN =
      Pattern.compile("mm2-configs\\.(.+)\\.internal");

  private static final String MIRROR_SOURCE_CONNECTOR_KEY = "connector-MirrorSourceConnector";
  private static final String SOURCE_BOOTSTRAP_FIELD      = "source.cluster.bootstrap.servers";

  private final AdminClientService  adminClientService;
  private final ClustersStorage     clustersStorage;
  private final ConsumerGroupService consumerGroupService;


  public Mono<Map<String, Long>> computeLagForCluster(KafkaCluster targetCluster) {
    return adminClientService.get(targetCluster)
        .flatMap(ac -> ac.listTopics(true))
        .flatMap(allTopics -> {

          // Collect all sync topics and their aliases

          Map<String, String> syncTopicToAlias = allTopics.stream()
              .filter(t -> SYNC_TOPIC_PATTERN.matcher(t).matches())
              .collect(Collectors.toMap(
                  t -> t,
                  t -> { Matcher m = SYNC_TOPIC_PATTERN.matcher(t); m.matches(); return m.group(1); }
              ));

          if (syncTopicToAlias.isEmpty()) {
            log.debug("No MM2 offset-sync topics found on cluster {}", targetCluster.getName());
            return Mono.just(Collections.<String, Long>emptyMap());
          }

          log.debug("Found MM2 offset-sync topics on {}: {}",
              targetCluster.getName(), syncTopicToAlias.keySet());

          return Mono.fromCallable(
                  () -> computeLagFromAllConnectors(targetCluster, syncTopicToAlias, allTopics))
              .subscribeOn(Schedulers.boundedElastic());
        })
        .onErrorResume(e -> {
          log.warn("Failed to compute MM2 lag for cluster {}: {}",
              targetCluster.getName(), e.getMessage());
          return Mono.just(Collections.emptyMap());
        });
  }


  /**
   * Processes each MM2 connector (identified by alias) independently:
   * reads its own offset-syncs topic, finds its own source bootstrap,
   * fetches LEOs from that specific source, computes lag.
   * Merges all results -- if multiple connectors contribute to the same
   * topic, their lag values are summed because each connector represents
   * an independent source of unsynced messages.
   * the higher lag wins (conservative).
   */
  private Map<String, Long> computeLagFromAllConnectors(KafkaCluster targetCluster,
                                                        Map<String, String> syncTopicToAlias,
                                                        Set<String> allTargetTopics) {
    // Build alias -> configs-topic map for quick lookup

    Map<String, String> aliasToConfigsTopic = allTargetTopics.stream()
        .filter(t -> CONFIGS_TOPIC_PATTERN.matcher(t).matches())
        .collect(Collectors.toMap(
            t -> { Matcher m = CONFIGS_TOPIC_PATTERN.matcher(t); m.matches(); return m.group(1); },
            t -> t
        ));

    log.debug("Alias -> configs topic map for {}: {}", targetCluster.getName(), aliasToConfigsTopic);

    // Final merged result across all connectors
    Map<String, Long> merged = new HashMap<>();

    for (Map.Entry<String, String> entry : syncTopicToAlias.entrySet()) {
      String syncTopic = entry.getKey();   // mm2-offset-syncs.src.internal
      String alias     = entry.getValue(); // src

      log.debug("Processing connector alias='{}' syncTopic='{}' on {}",
          alias, syncTopic, targetCluster.getName());

      // Step 1: read offset-syncs for this connector
      Map<String, Map<Integer, Long>> syncedOffsets;
      try (EnhancedConsumer consumer = consumerGroupService.createConsumer(targetCluster)) {
        syncedOffsets = readOffsetSyncsTopic(consumer, syncTopic);
      } catch (Exception e) {
        log.error("Error reading {} on {}: {}", syncTopic, targetCluster.getName(), e.getMessage());
        continue;
      }

      if (syncedOffsets.isEmpty()) {
        log.debug("No synced offsets found in {} - skipping", syncTopic);
        continue;
      }

      // Step 2: get source bootstrap for THIS alias specifically
      String configsTopic = aliasToConfigsTopic.get(alias);
      Map<String, Long> connectorLag = computeLagForConnector(
          targetCluster, alias, configsTopic, syncedOffsets);

      // Step 3: merge into overall result - SUM lag across all connectors
      // because each connector contributes independent unsynced messages.
      // e.g. site1 topic lag=200 + site2 topic lag=200 = 400 total missing from target
      connectorLag.forEach((topic, lag) ->
          merged.merge(topic, lag, Long::sum));
    }

    return merged;
  }

  //  Compute lag for one connector

  private Map<String, Long> computeLagForConnector(KafkaCluster targetCluster,
                                                   String alias,
                                                   String configsTopic,
                                                   Map<String, Map<Integer, Long>> syncedOffsets) {
    // Strategy 1: read source bootstrap from THIS connector's configs topic
    if (configsTopic != null) {
      Optional<String> sourceBootstrap = readBootstrapForAlias(targetCluster, configsTopic, alias);
      if (sourceBootstrap.isPresent()) {
        Map<String, Long> result = computeLagUsingBootstrap(
            sourceBootstrap.get(), syncedOffsets, alias);
        if (!result.isEmpty()) {
          return result;
        }
        log.warn("alias='{}' bootstrap={} found but LEO fetch failed -- falling back",
            alias, sourceBootstrap.get());
      }
    } else {
      log.debug("No mm2-configs topic found for alias='{}' -- skipping bootstrap strategy", alias);
    }

    // Strategy 2: find source among configured clusters (with target guard)
    KafkaCluster sourceCluster = findSourceCluster(
        targetCluster, syncedOffsets.keySet());
    if (sourceCluster != null) {
      log.debug("alias='{}' -- using configured source cluster {}", alias, sourceCluster.getName());
      return computeLagUsingSourceLeo(sourceCluster, syncedOffsets);
    }

    // Strategy 3: fallback -- target LEOs
    log.debug("alias='{}' -- falling back to target LEO comparison", alias);
    return computeLagUsingTargetLeo(targetCluster, syncedOffsets);
  }

  //  Read bootstrap for a specific alias

  /**
   * Reads {@code mm2-configs.{alias}.internal} and extracts
   * {@code source.cluster.bootstrap.servers} for that specific connector.
   * Each alias has its own configs topic - correct bootstrap per connector.
   */
  private Optional<String> readBootstrapForAlias(KafkaCluster targetCluster,
                                                 String configsTopic,
                                                 String alias) {
    try (EnhancedConsumer consumer = consumerGroupService.createConsumer(targetCluster)) {
      return readBootstrapFromConfigsTopic(consumer, configsTopic, alias);
    } catch (Exception e) {
      log.debug("Error reading configs topic {} for alias='{}': {}",
          configsTopic, alias, e.getMessage());
      return Optional.empty();
    }
  }

  private Optional<String> readBootstrapFromConfigsTopic(EnhancedConsumer consumer,
                                                         String configsTopic,
                                                         String alias) {
    try {
      List<TopicPartition> partitions = consumer.partitionsFor(configsTopic).stream()
          .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
          .collect(Collectors.toList());

      if (partitions.isEmpty()) return Optional.empty();

      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);

      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
      long totalToRead = endOffsets.values().stream().mapToLong(Long::longValue).sum();
      if (totalToRead == 0) return Optional.empty();

      long recordsRead = 0;
      while (recordsRead < totalToRead) {
        ConsumerRecords<Bytes, Bytes> records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) break;

        for (ConsumerRecord<Bytes, Bytes> record : records) {
          recordsRead++;
          if (record.key() == null || record.value() == null) continue;

          String key = new String(record.key().get());
          if (key.startsWith(MIRROR_SOURCE_CONNECTOR_KEY)) {
            try {
              JsonNode props = MAPPER.readTree(new String(record.value().get()))
                  .path("properties");
              JsonNode bootstrapNode = props.path(SOURCE_BOOTSTRAP_FIELD);
              if (!bootstrapNode.isMissingNode() && !bootstrapNode.isNull()) {
                String bootstrap = bootstrapNode.asText();
                if (!bootstrap.isBlank()) {
                  log.debug("alias='{}' extracted source bootstrap from {}: {}",
                      alias, configsTopic, bootstrap);
                  return Optional.of(bootstrap);
                }
              }
            } catch (Exception e) {
              log.trace("Could not parse configs record from {}: {}", configsTopic, e.getMessage());
            }
          }
        }
      }
    } catch (Exception e) {
      log.warn("Error reading configs topic {} for alias='{}': {}",
          configsTopic, alias, e.getMessage());
    }
    return Optional.empty();
  }

  //  Compute lag using bootstrap string (temp AdminClient)

  private Map<String, Long> computeLagUsingBootstrap(String sourceBootstrap,
                                                     Map<String, Map<Integer, Long>> syncedOffsets,
                                                     String alias) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, sourceBootstrap);
    props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    props.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");

    try (AdminClient adminClient = AdminClient.create(props)) {
      List<TopicPartition> tps = new ArrayList<>();
      syncedOffsets.forEach((topic, partMap) ->
          partMap.keySet().forEach(part -> tps.add(new TopicPartition(topic, part))));

      var listOffsetsResult = adminClient.listOffsets(
          tps.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())));

      Map<TopicPartition, Long> sourceLeos = new HashMap<>();
      for (TopicPartition tp : tps) {
        try {
          long offset = listOffsetsResult.partitionResult(tp)
              .get(10, java.util.concurrent.TimeUnit.SECONDS)
              .offset();
          sourceLeos.put(tp, offset);
        } catch (Exception e) {
          log.trace("alias='{}' could not fetch LEO for {} from {}: {}",
              alias, tp, sourceBootstrap, e.getMessage());
        }
      }

      if (sourceLeos.isEmpty()) {
        log.warn("alias='{}' no LEOs fetched from {} -- all partitions failed",
            alias, sourceBootstrap);
        return Collections.emptyMap();
      }

      log.debug("alias='{}' fetched {} LEOs from {}", alias, sourceLeos.size(), sourceBootstrap);
      return computeLagFromLeos(syncedOffsets, sourceLeos);

    } catch (Exception e) {
      log.warn("alias='{}' failed to fetch LEOs from {}: {}", alias, sourceBootstrap, e.getMessage());
      return Collections.emptyMap();
    }
  }

  //  Strategy 2: find source among configured clusters

  private KafkaCluster findSourceCluster(KafkaCluster targetCluster,
                                         Set<String> syncedTopics) {
    for (KafkaCluster candidate : clustersStorage.getKafkaClusters()) {
      if (candidate.getName().equals(targetCluster.getName())) continue;
      try {
        Set<String> candidateTopics = adminClientService.get(candidate)
            .flatMap(ac -> ac.listTopics(true))
            .block(Duration.ofSeconds(5));

        if (candidateTopics == null) continue;

        // Guard: skip clusters that also have offset-sync topics -- they are targets
        boolean candidateIsAlsoTarget = candidateTopics.stream()
            .anyMatch(t -> OffsetSyncSerde.TOPIC_NAME_PATTERN.matcher(t).matches());
        if (candidateIsAlsoTarget) {
          log.debug("Skipping {} -- has offset-sync topics, treating as target not source",
              candidate.getName());
          continue;
        }

        if (!Collections.disjoint(candidateTopics, syncedTopics)) {
          return candidate;
        }
      } catch (Exception e) {
        log.debug("Could not check cluster {} as MM2 source: {}",
            candidate.getName(), e.getMessage());
      }
    }
    return null;
  }

  //  Strategy 3: compute lag using source cluster AdminClient

  private Map<String, Long> computeLagUsingSourceLeo(KafkaCluster sourceCluster,
                                                     Map<String, Map<Integer, Long>> syncedOffsets) {
    try {
      List<TopicPartition> tps = new ArrayList<>();
      syncedOffsets.forEach((topic, partMap) ->
          partMap.keySet().forEach(part -> tps.add(new TopicPartition(topic, part))));

      Map<TopicPartition, Long> sourceLeos = adminClientService.get(sourceCluster)
          .flatMap(ac -> ac.listOffsets(tps, OffsetSpec.latest(), false))
          .block(Duration.ofSeconds(10));

      if (sourceLeos == null) return Collections.emptyMap();
      return computeLagFromLeos(syncedOffsets, sourceLeos);
    } catch (Exception e) {
      log.warn("Error fetching source LEOs for MM2 lag: {}", e.getMessage());
    }
    return Collections.emptyMap();
  }

  //  Fallback: compute lag using target LEOs

  private Map<String, Long> computeLagUsingTargetLeo(KafkaCluster targetCluster,
                                                     Map<String, Map<Integer, Long>> syncedOffsets) {
    try {
      List<TopicPartition> tps = new ArrayList<>();
      syncedOffsets.forEach((topic, partMap) ->
          partMap.keySet().forEach(part -> tps.add(new TopicPartition(topic, part))));

      Map<TopicPartition, Long> targetLeos = adminClientService.get(targetCluster)
          .flatMap(ac -> ac.listOffsets(tps, OffsetSpec.latest(), false))
          .block(Duration.ofSeconds(10));

      if (targetLeos == null) return Collections.emptyMap();
      return computeLagFromLeos(syncedOffsets, targetLeos);
    } catch (Exception e) {
      log.warn("Error fetching target LEOs for MM2 lag fallback: {}", e.getMessage());
    }
    return Collections.emptyMap();
  }

  //  Shared lag formula

  /**
   * lag = max(0, LEO - (upstreamOffset + 1))
   *
   * +1 because upstreamOffset is inclusive (last copied offset, 0-based)
   * while LEO is exclusive (next write position).
   */
  private Map<String, Long> computeLagFromLeos(Map<String, Map<Integer, Long>> syncedOffsets,
                                               Map<TopicPartition, Long> leos) {
    Map<String, Long> result = new HashMap<>();
    syncedOffsets.forEach((topic, partMap) -> {
      long topicLag = 0;
      for (Map.Entry<Integer, Long> e : partMap.entrySet()) {
        TopicPartition tp = new TopicPartition(topic, e.getKey());
        Long leo = leos.get(tp);
        if (leo != null) {
          topicLag += Math.max(0, leo - (e.getValue() + 1));
        }
      }
      result.put(topic, topicLag);
    });
    return result;
  }

  //  Read offset-syncs topic

  private Map<String, Map<Integer, Long>> readOffsetSyncsTopic(EnhancedConsumer consumer,
                                                               String syncTopic) {
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
            Struct key            = OFFSET_SYNC_SERDE.deserializeKey(record.key().get());
            Struct value          = OFFSET_SYNC_SERDE.deserializeValue(record.value().get());
            String sourceTopic    = (String) key.get("topic");
            int    partition      = (int)    key.get("partition");
            long   upstreamOffset = (long)   value.get("upstreamOffset");

            result.computeIfAbsent(sourceTopic, k -> new HashMap<>())
                .merge(partition, upstreamOffset, Math::max);
          } catch (Exception e) {
            log.trace("Could not decode offset-sync record from {}: {}",
                syncTopic, e.getMessage());
          }
        }
      }

      log.debug("Read {} records from {}, found {} source topics",
          recordsRead, syncTopic, result.size());
    } catch (Exception e) {
      log.warn("Error reading offset-sync topic {}: {}", syncTopic, e.getMessage());
    }
    return result;
  }
}
