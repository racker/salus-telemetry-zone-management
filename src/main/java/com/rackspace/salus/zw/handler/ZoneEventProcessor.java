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

import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.zw.services.ZoneStorageListener;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.watch.WatchResponse;
import java.util.function.Consumer;

public abstract class ZoneEventProcessor implements Consumer<WatchResponse> {
  Watcher zoneWatcher;
  ZoneStorageListener listener;
  ZoneStorage zoneStorage;

  ZoneEventProcessor(ZoneStorageListener listener, ZoneStorage zoneStorage) {
    this.listener = listener;
    this.zoneStorage = zoneStorage;
  }

  public void setZoneWatcher(Watcher zoneWatcher) {
    this.zoneWatcher = zoneWatcher;
  }
}
