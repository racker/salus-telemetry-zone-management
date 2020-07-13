/*
 * Copyright 2020 Rackspace US, Inc.
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

package com.rackspace.salus.zw.services;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.monitor_management.web.client.ZoneApi;
import com.rackspace.salus.monitor_management.web.model.ZoneDTO;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.telemetry.messaging.ExpiredResourceZoneEvent;
import com.rackspace.salus.telemetry.messaging.NewResourceZoneEvent;
import com.rackspace.salus.telemetry.messaging.ReattachedResourceZoneEvent;
import io.etcd.jetcd.Watch.Watcher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * This class utilities {@link ZoneStorage} to register and react to etcd watches on the ranges
 * of zone-related keys. It is essentially a mediator between etcd events and the production
 * of kafka events for consumption by other microservices in Salus.
 */
@Service
@Slf4j
public class ZoneWatchingService implements ZoneStorageListener {

  static final long NULL_ZONE_POLLER_TIMEOUT = 30;
  static final long FALLBACK_POLLER_TIMEOUT = 300;

  private final ZoneStorage zoneStorage;
  private final KafkaTemplate kafkaTemplate;
  private final KafkaTopicProperties topicProperties;
  private final ZoneApi zoneApi;
  private final EtcdWatchConnector etcdWatchConnector;
  private final Counter retrievePollerTimeoutErrors;
  private Watcher expectedZonesWatcher;
  private Watcher activeZonesWatcher;
  private Watcher expiringZonesWatcher;

  @Autowired
  public ZoneWatchingService(ZoneStorage zoneStorage, KafkaTemplate kafkaTemplate,
      MeterRegistry meterRegistry,
      KafkaTopicProperties topicProperties,
      ZoneApi zoneApi, EtcdWatchConnector etcdWatchConnector) {
    this.zoneStorage = zoneStorage;
    this.kafkaTemplate = kafkaTemplate;
    this.topicProperties = topicProperties;
    this.zoneApi = zoneApi;
    this.etcdWatchConnector = etcdWatchConnector;

    retrievePollerTimeoutErrors = meterRegistry
        .counter("errors", "operation", "retrievePollerTimeout");
  }

  @PostConstruct
  public void start() {
    etcdWatchConnector.watchExpectedZones(this)
      .thenAccept(watcher -> {
        log.debug("Watching expected zones");
        this.expectedZonesWatcher = watcher;
      });
    etcdWatchConnector.watchActiveZones(this)
      .thenAccept(watcher -> {
        log.debug("Watching active zones");
        this.activeZonesWatcher = watcher;
      });
    etcdWatchConnector.watchExpiringZones(this)
        .thenAccept(watcher -> {
          log.debug("Watching expiring zones");
          this.expiringZonesWatcher = watcher;
        });
  }

  @PreDestroy
  public void stop() {
    if (expectedZonesWatcher != null) {
      expectedZonesWatcher.close();
      expectedZonesWatcher = null;
    }
    if (activeZonesWatcher != null) {
      activeZonesWatcher.close();
      activeZonesWatcher = null;
    }
    if (expiringZonesWatcher != null) {
      expiringZonesWatcher.close();
      expiringZonesWatcher = null;
    }
  }

  private String buildMessageKey(ResolvedZone resolvedZone) {
    return resolvedZone.isPublicZone() ?
        resolvedZone.getName() :
        String.join(":", resolvedZone.getTenantId(), resolvedZone.getName());
  }


  @Override
  public void handleNewEnvoyResourceInZone(ResolvedZone resolvedZone, String resourceId) {
    log.debug("Handling new envoy-resource in zone={} resource={}", resolvedZone, resourceId);

    //noinspection unchecked
    kafkaTemplate.send(
        topicProperties.getZones(),
        buildMessageKey(resolvedZone),
        new NewResourceZoneEvent()
            .setResourceId(resourceId)
            .setTenantId(resolvedZone.getTenantId())
            .setZoneName(resolvedZone.getName())
    );
  }

  @Override
  public void handleEnvoyResourceReassignedInZone(ResolvedZone resolvedZone, String resourceId,
                                                  String fromEnvoyId, String toEnvoyId) {
    log.debug("Handling new envoy-resource reassigned zone={} from={} to={}",
        resolvedZone, fromEnvoyId, toEnvoyId);

    //noinspection unchecked
    kafkaTemplate.send(
        topicProperties.getZones(),
        buildMessageKey(resolvedZone),
        new ReattachedResourceZoneEvent()
            .setFromEnvoyId(fromEnvoyId)
            .setToEnvoyId(toEnvoyId)
            .setResourceId(resourceId)
            .setTenantId(resolvedZone.getTenantId())
            .setZoneName(resolvedZone.getName())
    );
  }

  @Override
  public void handleActiveEnvoyConnection(ResolvedZone resolvedZone, String resourceId) {
    log.debug("Handling new active envoy connection for zone={} resource={}", resolvedZone, resourceId);
    // look for timer and remove it
    zoneStorage.removeExpiringEntry(resolvedZone, resourceId).join();
  }

  @Override
  public void handleActiveEnvoyDisconnection(ResolvedZone resolvedZone, String resourceId, String envoyId) {
    log.debug("Handling active envoy disconnection for zone={} resource={}", resolvedZone, resourceId);

    long pollerTimeout;
    try {
      ZoneDTO zone = zoneApi.getByZoneName(resolvedZone.getTenantId(), resolvedZone.getName());
      if (zone == null) {
        log.debug("Envoy disconnected from zone that does not exist anymore {}", resolvedZone);
        retrievePollerTimeoutErrors.increment();
        pollerTimeout = NULL_ZONE_POLLER_TIMEOUT;
      } else {
        pollerTimeout = zone.getPollerTimeout();
      }
    } catch (IllegalArgumentException e) {
      log.warn("Call to get poller timeout by zone name failed; using fallback timeout", e);
      retrievePollerTimeoutErrors.increment();
      pollerTimeout = FALLBACK_POLLER_TIMEOUT;
    }

    zoneStorage.createExpiringEntry(resolvedZone, resourceId, envoyId, pollerTimeout).join();
  }

  @Override
  public void handleExpiredEnvoy(ResolvedZone resolvedZone, String resourceId, String envoyId) {
    log.debug("Handling expired envoy for zone={} resource={} envoy={}", resolvedZone, resourceId, envoyId);
    zoneStorage.removeExpectedEntry(resolvedZone, resourceId).join();
    // send event to monitor management to reassign monitors.
    kafkaTemplate.send(
        topicProperties.getZones(),
        buildMessageKey(resolvedZone),
        new ExpiredResourceZoneEvent()
            .setEnvoyId(envoyId)
            .setTenantId(resolvedZone.getTenantId())
            .setZoneName(resolvedZone.getName())
    );
  }

}