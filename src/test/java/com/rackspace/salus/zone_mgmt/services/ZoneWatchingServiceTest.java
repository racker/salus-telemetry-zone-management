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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Watch.Watcher;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.telemetry.messaging.ZoneEnvoyOfResourceChangedEvent;
import com.rackspace.salus.telemetry.messaging.ZoneNewResourceEvent;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.kafka.core.KafkaTemplate;

@RunWith(MockitoJUnitRunner.class)
public class ZoneWatchingServiceTest {

  @Mock
  KafkaTemplate kafkaTemplate;

  @Mock
  ZoneStorage zoneStorage;

  @Mock
  Watcher watcher;

  @Test
  public void testStart() {
    when(zoneStorage.watchExpectedZones(any()))
        .thenReturn(CompletableFuture.completedFuture(watcher));

    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, topicProperties);

    zoneWatchingService.start();

    verify(zoneStorage).watchExpectedZones(same(zoneWatchingService));
  }

  @Test
  public void handleNewEnvoyResourceInZone() {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, topicProperties);

    final ResolvedZone resolvedZone = new ResolvedZone()
        .setTenantId("t-1")
        .setId("z-1");

    zoneWatchingService.handleNewEnvoyResourceInZone(resolvedZone);

    //noinspection unchecked
    verify(kafkaTemplate).send(
        eq("test.zones.json"),
        eq("t-1:z-1"),
        eq(
            new ZoneNewResourceEvent()
                .setTenantId("t-1")
                .setZoneId("z-1")
        )
    );

    verifyNoMoreInteractions(kafkaTemplate);
  }

  @Test
  public void handleEnvoyResourceReassignedInZone() {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, topicProperties);

    final ResolvedZone resolvedZone = new ResolvedZone()
        .setTenantId("t-1")
        .setId("z-1");

    zoneWatchingService.handleEnvoyResourceReassignedInZone(resolvedZone, "e-1", "e-2");

    //noinspection unchecked
    verify(kafkaTemplate).send(
        eq("test.zones.json"),
        eq("t-1:z-1"),
        eq(
            new ZoneEnvoyOfResourceChangedEvent()
                .setFromEnvoyId("e-1")
                .setToEnvoyId("e-2")
                .setTenantId("t-1")
                .setZoneId("z-1")
        )
    );

    verifyNoMoreInteractions(kafkaTemplate);

  }
}