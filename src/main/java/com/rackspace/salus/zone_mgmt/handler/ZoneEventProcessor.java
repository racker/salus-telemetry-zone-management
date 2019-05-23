package com.rackspace.salus.zone_mgmt.handler;

import com.coreos.jetcd.Watch.Watcher;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.zone_mgmt.services.ZoneStorageListener;

public abstract class ZoneEventProcessor implements Runnable {
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
