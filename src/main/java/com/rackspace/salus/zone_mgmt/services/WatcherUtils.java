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

import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_EXPIRING;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPIRING;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.options.WatchOption.Builder;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.zone_mgmt.handler.ActiveZoneEventProcessor;
import com.rackspace.salus.zone_mgmt.handler.ExpectedZoneEventProcessor;
import com.rackspace.salus.zone_mgmt.handler.ExpiringZoneEventProcessor;
import com.rackspace.salus.zone_mgmt.handler.ZoneEventProcessor;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

@Slf4j
@Service
public class WatcherUtils {

  private final ZoneStorage zoneStorage;

  public WatcherUtils(ZoneStorage zoneStorage) {
    this.zoneStorage = zoneStorage;
  }

  /**
   * Sets up asynchronous watching of the expected zone key range.
   *
   * @param listener the listener that will be invoked when changes related to expected zones occur
   * @return a future that is completed when the watcher is setup and being processed. The contained
   * {@link Watcher} is provided only for testing/informational purposes.
   */
  public CompletableFuture<Watcher> watchExpectedZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_EXPECTED, PREFIX_ZONE_EXPECTED, listener,
        new ExpectedZoneEventProcessor(listener, zoneStorage));
  }

  public CompletableFuture<Watcher> watchActiveZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_ACTIVE, PREFIX_ZONE_ACTIVE, listener,
        new ActiveZoneEventProcessor(listener, zoneStorage));
  }

  public CompletableFuture<Watcher> watchExpiringZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_EXPIRING, PREFIX_ZONE_EXPIRING, listener,
        new ExpiringZoneEventProcessor(listener, zoneStorage));
  }

  private CompletableFuture<Watcher> watchZones(String trackingKey, String watchPrefixStr,
      ZoneStorageListener listener, ZoneEventProcessor watchEventHandler) {
    Assert.notNull(listener, "A ZoneStorageListener is required");

    // first we need to see if a previous app was watching the zones
    return
        zoneStorage.getRevisionOfKey(trackingKey)
            .thenApply(watchRevision -> {
              log.debug("Watching {} from revision {}", watchPrefixStr, watchRevision);

              final ByteSequence watchPrefix = ByteSequence
                  .fromString(watchPrefixStr);

              final Builder watchOptionBuilder = WatchOption.newBuilder()
                  .withPrefix(watchPrefix)
                  .withPrevKV(true)
                  .withRevision(watchRevision);

              final Watcher zoneWatcher = zoneStorage.getWatchClient().watch(
                  watchPrefix,
                  watchOptionBuilder.build()
              );

              watchEventHandler.setZoneWatcher(zoneWatcher);

              new Thread(
                  watchEventHandler,
                  "zoneWatcher")
                  .start();

              return zoneWatcher;
            });
  }
}
