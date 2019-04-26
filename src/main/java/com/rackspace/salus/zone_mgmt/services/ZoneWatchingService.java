/*
 * Copyright 2019 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.salus.zone_mgmt.services;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.common.exception.EtcdException;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorageListener;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.telemetry.messaging.ZoneEnvoyOfResourceChangedEvent;
import com.rackspace.salus.telemetry.messaging.ZoneNewResourceEvent;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ZoneWatchingService implements ZoneStorageListener {

  private final ZoneStorage zoneStorage;
  private final KafkaTemplate kafkaTemplate;
  private final KafkaTopicProperties topicProperties;
  private Watcher expectedZonesWatcher;

  @Autowired
  public ZoneWatchingService(ZoneStorage zoneStorage, KafkaTemplate kafkaTemplate,
                             KafkaTopicProperties topicProperties) {
    this.zoneStorage = zoneStorage;
    this.kafkaTemplate = kafkaTemplate;
    this.topicProperties = topicProperties;
  }

  @PostConstruct
  public void start() {

    zoneStorage.watchExpectedZones(this)
        .thenAccept(watcher -> {
          log.debug("Watching expected zones");
          this.expectedZonesWatcher = watcher;
        })
        .join();

  }

  @PreDestroy
  public void stop() {
    if (expectedZonesWatcher != null) {
      expectedZonesWatcher.close();
      expectedZonesWatcher = null;
    }
  }

  @Override
  public void handleNewEnvoyResourceInZone(ResolvedZone resolvedZone) {
    log.debug("Handling new envoy-resource in zone={}", resolvedZone);

    //noinspection unchecked
    kafkaTemplate.send(
        topicProperties.getZones(),
        buildMessageKey(resolvedZone),
        new ZoneNewResourceEvent()
            .setTenantId(resolvedZone.getTenantId())
            .setZoneId(resolvedZone.getId())
    );
  }

  private String buildMessageKey(ResolvedZone resolvedZone) {
    return resolvedZone.isPublicZone() ?
        resolvedZone.getId() :
        String.join(":", resolvedZone.getTenantId(), resolvedZone.getId());
  }

  @Override
  public void handleEnvoyResourceReassignedInZone(ResolvedZone resolvedZone, String fromEnvoyId,
                                                  String toEnvoyId) {
    log.debug("Handling new envoy-resource reassigned zone={} from={} to={}",
        resolvedZone, fromEnvoyId, toEnvoyId);

    //noinspection unchecked
    kafkaTemplate.send(
        topicProperties.getZones(),
        buildMessageKey(resolvedZone),
        new ZoneEnvoyOfResourceChangedEvent()
            .setFromEnvoyId(fromEnvoyId)
            .setToEnvoyId(toEnvoyId)
            .setTenantId(resolvedZone.getTenantId())
            .setZoneId(resolvedZone.getId())
    );
  }

  @Override
  public void handleExpectedZoneWatcherClosed(EtcdException e) {
    // this is normal during application shutdown
    log.debug("Observed closure while watching expected zones: {}", e.getMessage());
  }
}
