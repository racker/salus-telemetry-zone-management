package com.rackspace.salus.zw.web.controller;

import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.PrivateZoneName;
import com.rackspace.salus.telemetry.etcd.types.PublicZoneName;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api")
public class ZoneApiController {

  ZoneStorage zoneStorage;

  @Autowired
  public ZoneApiController(ZoneStorage zoneStorage) {
    this.zoneStorage = zoneStorage;
  }

  @GetMapping("/admin/zone/{zone}/detached-pollers")
  public CompletableFuture<List<String>> getExpiredPollerAdmin(
      @PathVariable @PublicZoneName String zone) {

    ResolvedZone resolvedZone = ResolvedZone.createPublicZone(zone);
    return zoneStorage.getExpiredPollerResourceIdsInZone(resolvedZone);
  }

  @GetMapping("/tenant/{tenantId}/zone/{zone}/detaches-pollers")
  public CompletableFuture<List<String>> getExpiredPollerPublic(@PathVariable String tenantId,
      @PathVariable @PrivateZoneName String zone) {

    ResolvedZone resolvedZone = ResolvedZone.createPrivateZone(tenantId, zone);
    return zoneStorage.getExpiredPollerResourceIdsInZone(resolvedZone);
  }
}
