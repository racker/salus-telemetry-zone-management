package com.rackspace.salus.zw.web.controller;

import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.PrivateZoneName;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.HandlerMapping;

@Slf4j
@RestController
@RequestMapping("/api")
public class ZoneApiController {

  ZoneStorage zoneStorage;

  @Autowired
  public ZoneApiController(ZoneStorage zoneStorage) {
    this.zoneStorage = zoneStorage;
  }

  @GetMapping("/admin/zone/**")
  public CompletableFuture<List<String>> getExpiredPollerAdmin(HttpServletRequest request) {
    String zone = extractPublicZoneNameFromUri(request);
    ResolvedZone resolvedZone = ResolvedZone.createPublicZone(zone);
    return zoneStorage.getExpiredPollerResourceIdsInZone(resolvedZone);
  }

  @GetMapping("/tenant/{tenantId}/zone/{zone}/detached-pollers")
  public CompletableFuture<List<String>> getExpiredPollerPublic(@PathVariable String tenantId,
      @PathVariable @PrivateZoneName String zone) {

    ResolvedZone resolvedZone = ResolvedZone.createPrivateZone(tenantId, zone);
    return zoneStorage.getExpiredPollerResourceIdsInZone(resolvedZone);
  }

  /**
   * Discovers the zone name within the uri. Handles both public zones containing slashes as well as
   * more simple private zone. Using @PathVariable does not work for public zones.
   *
   * @param request The incoming http request.
   * @return The zone name provided within the uri.
   */
  private String extractZoneNameFromUri(HttpServletRequest request) {
    // For example, /api/admin//zones/public/region_1
    String path = (String) request
        .getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
    // For example, /api/admin/zones/**
    String bestMatchPattern = (String) request
        .getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
    // For example, public/region_1
    return new AntPathMatcher().extractPathWithinPattern(bestMatchPattern, path);
  }

  /**
   * Helper method that calls {@link #extractZoneNameFromUri(HttpServletRequest)} but also validates
   * the zone name is a legal public zone name.
   *
   * @param request The incoming http request.
   * @return The zone name provided within the uri.
   */
  private String extractPublicZoneNameFromUri(HttpServletRequest request) {
    final String name = extractZoneNameFromUri(request);
    if (!name.startsWith(ResolvedZone.PUBLIC_PREFIX)) {
      throw new IllegalArgumentException("Must provide a public zone name");
    }
    return name;
  }
}
