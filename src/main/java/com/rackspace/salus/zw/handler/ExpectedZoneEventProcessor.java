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

package com.rackspace.salus.zw.handler;

import static com.rackspace.salus.telemetry.etcd.types.Keys.PTN_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPECTED;

import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.zw.services.ZoneStorageListener;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchEvent.EventType;
import io.etcd.jetcd.watch.WatchResponse;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import lombok.extern.slf4j.Slf4j;


/**
 * This is used to handle any etcd watcher events that get triggered relating
 * to the expected zone key prefix.
 */
@Slf4j
public class ExpectedZoneEventProcessor extends ZoneEventProcessor {
  final ByteSequence trackingKey = EtcdUtils.fromString(TRACKING_KEY_ZONE_EXPECTED);

  public ExpectedZoneEventProcessor(ZoneStorageListener listener, ZoneStorage zoneStorage) {
    super(listener, zoneStorage);
  }

  /*
  tenantId and resourceId are being deserialized from the etcd key.
  Since the etcd key is being lowercase'ed the values may be a different case than what is actually being stored in MySQL
   */
  @Override
  public void accept(WatchResponse response) {
    try {
      for (WatchEvent event : response.getEvents()) {

        final EventType eventType = event.getEventType();

        switch (eventType) {
          case DELETE:
            // no action necessary
            break;
          case PUT:
            final String keyStr = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
            final Matcher matcher = PTN_ZONE_EXPECTED.matcher(keyStr);

            if (!matcher.matches()) {
              log.warn("Unable to parse expected event key={}", keyStr);
              continue;
            }
            String resourceId = matcher.group("resourceId");
            final ResolvedZone resolvedZone = ResolvedZone.fromKeyParts(
                matcher.group("tenant"),
                matcher.group("zoneName")
            );

            // prev KV is always populated by event, but a version=0 means it wasn't present in storage
            if (event.getPrevKV().getVersion() == 0) {
              try {
                listener.handleNewEnvoyResourceInZone(resolvedZone, resourceId);
              } catch (Exception e) {
                log.warn("Unexpected failure within listener={}", listener, e);
              }
            } else {
              try {
                listener.handleEnvoyResourceReassignedInZone(
                    resolvedZone,
                    resourceId,
                    event.getPrevKV().getValue().toString(StandardCharsets.UTF_8),
                    event.getKeyValue().getValue().toString(StandardCharsets.UTF_8)
                );
              } catch (Exception e) {
                log.warn("Unexpected failure within listener={}", listener, e);
              }
            }
            break;
          case UNRECOGNIZED:
            log.warn("Unknown expected watcher event seen by zone watcher");
            break;
        }
      }
    } finally {
      zoneStorage.incrementTrackingKeyVersion(trackingKey);
    }
  }
}
