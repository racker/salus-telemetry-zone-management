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

package com.rackspace.salus.zw.services;

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.EXIT_CODE_ETCD_FAILED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_EXPIRING;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPIRING;

import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.zw.handler.ActiveZoneEventProcessor;
import com.rackspace.salus.zw.handler.ExpectedZoneEventProcessor;
import com.rackspace.salus.zw.handler.ExpiringZoneEventProcessor;
import com.rackspace.salus.zw.handler.ZoneEventProcessor;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.options.WatchOption.Builder;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

@Slf4j
@Service
public class EtcdWatchConnector {

  private final ZoneStorage zoneStorage;

  public EtcdWatchConnector(ZoneStorage zoneStorage) {
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

  /**
   * Sets up asynchronous watching of the active zone key range.
   *
   * @param listener the listener that will be invoked when changes related to active zones occur
   * @return a future that is completed when the watcher is setup and being processed. The contained
   * {@link Watcher} is provided only for testing/informational purposes.
   */
  public CompletableFuture<Watcher> watchActiveZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_ACTIVE, PREFIX_ZONE_ACTIVE, listener,
        new ActiveZoneEventProcessor(listener, zoneStorage));
  }

  /**
   * Sets up asynchronous watching of the expiring zone key range.
   *
   * @param listener the listener that will be invoked when changes related to expiring zones occur
   * @return a future that is completed when the watcher is setup and being processed. The contained
   * {@link Watcher} is provided only for testing/informational purposes.
   */
  public CompletableFuture<Watcher> watchExpiringZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_EXPIRING, PREFIX_ZONE_EXPIRING, listener,
        new ExpiringZoneEventProcessor(listener, zoneStorage));
  }

  /**
   * Sets up asynchronous watching of the provided key range.
   *
   * @param listener the listener that will be invoked when changes related to the key range occur
   * @return a future that is completed when the watcher is setup and being processed. The contained
   * {@link Watcher} is provided only for testing/informational purposes.
   */
  private CompletableFuture<Watcher> watchZones(String trackingKey, String watchPrefixStr,
      ZoneStorageListener listener, ZoneEventProcessor watchEventHandler) {
    Assert.notNull(listener, "A ZoneStorageListener is required");

    // first we need to see if a previous app was watching the zones
    return
        zoneStorage.getRevisionOfKey(trackingKey)
            .thenApply(watchRevision -> {
              log.debug("Watching {} from revision {}", watchPrefixStr, watchRevision);

              final ByteSequence watchPrefix = EtcdUtils
                  .fromString(watchPrefixStr);

              final Builder watchOptionBuilder = WatchOption.newBuilder()
                  .withPrefix(watchPrefix)
                  .withPrevKV(true)
                  .withRevision(watchRevision);

              final Watcher zoneWatcher = zoneStorage.getWatchClient().watch(
                  watchPrefix,
                  watchOptionBuilder.build(),
                  watchEventHandler,
                  throwable -> handleWatchError(watchPrefixStr, throwable)
              );

              watchEventHandler.setZoneWatcher(zoneWatcher);

              return zoneWatcher;
            });
  }

  private void handleWatchError(String prefix, Throwable throwable) {
    log.error("Error during watch of {}", prefix, throwable);
    // Spring will gracefully shutdown via shutdown hook
    System.exit(EXIT_CODE_ETCD_FAILED);
  }
}