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

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.fromString;
import static com.rackspace.salus.telemetry.etcd.types.ResolvedZone.createPrivateZone;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.monitor_management.web.client.ZoneApi;
import com.rackspace.salus.monitor_management.web.model.ZoneDTO;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.services.EnvoyLeaseTracking;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.telemetry.messaging.ExpiredResourceZoneEvent;
import com.rackspace.salus.telemetry.messaging.NewResourceZoneEvent;
import com.rackspace.salus.telemetry.messaging.ReattachedResourceZoneEvent;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.options.LeaseOption;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

@RunWith(MockitoJUnitRunner.class)
@DirtiesContext
public class ZoneWatchingServiceTest {

  @Rule
  public final EtcdClusterResource etcd = new EtcdClusterResource("ZoneWatchingTest", 1);

  @Rule
  public TestName testName = new TestName();

  @Mock
  KafkaTemplate kafkaTemplate;

  @Mock
  private EnvoyLeaseTracking envoyLeaseTracking;

  private ZoneStorage zoneStorage;

  private EtcdWatchConnector etcdWatchConnector;

  @Mock
  ZoneApi zoneApi;

  private MeterRegistry meterRegistry = new SimpleMeterRegistry();

  private Client client;

  @Before
  public void setUp() {
    client = io.etcd.jetcd.Client.builder().endpoints(
        etcd.cluster().getClientEndpoints()
    ).build();

    zoneStorage = spy(new ZoneStorage(client, envoyLeaseTracking));

    etcdWatchConnector = new EtcdWatchConnector(zoneStorage);
  }

  @After
  public void tearDown() {
    zoneStorage.stop();
    client.close();
  }

  @Test
  public void testStart() {
    EtcdWatchConnector etcdWatchConnectorSpy = Mockito.spy(etcdWatchConnector);
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnectorSpy);

    zoneWatchingService.start();

    verify(etcdWatchConnectorSpy).watchExpectedZones(same(zoneWatchingService));
    verify(etcdWatchConnectorSpy).watchActiveZones(same(zoneWatchingService));
    verify(etcdWatchConnectorSpy).watchExpiringZones(same(zoneWatchingService));
  }

  @Test
  public void testHandleNewEnvoyResourceInZone() {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    final ResolvedZone resolvedZone = ResolvedZone.createPrivateZone("t-1", "z-1");

    zoneWatchingService.handleNewEnvoyResourceInZone(resolvedZone, "r-1");

    //noinspection unchecked
    verify(kafkaTemplate).send(
        eq("test.zones.json"),
        eq("t-1:z-1"),
        eq(
            new NewResourceZoneEvent()
                .setResourceId("r-1")
                .setTenantId("t-1")
                .setZoneName("z-1")
        )
    );

    verifyNoMoreInteractions(kafkaTemplate);
  }

  @Test
  public void testHandleEnvoyResourceReassignedInZone() {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    final ResolvedZone resolvedZone = ResolvedZone.createPrivateZone("t-1", "z-1");

    zoneWatchingService.handleEnvoyResourceReassignedInZone(
        resolvedZone, "r-1", "e-1", "e-2");

    //noinspection unchecked
    verify(kafkaTemplate).send(
        eq("test.zones.json"),
        eq("t-1:z-1"),
        eq(
            new ReattachedResourceZoneEvent()
                .setFromEnvoyId("e-1")
                .setToEnvoyId("e-2")
                .setResourceId("r-1")
                .setTenantId("t-1")
                .setZoneName("z-1")
        )
    );

    verifyNoMoreInteractions(kafkaTemplate);

  }

  @Test
  public void testHandleActiveEnvoyConnection() throws Exception{
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    final ResolvedZone zone = ResolvedZone.createPrivateZone("t-1", "z-1");

    String resourceId = RandomStringUtils.randomAlphabetic(10);
    String expiringKey = new String(EtcdUtils.buildKey(Keys.FMT_ZONE_EXPIRING,
        zone.getTenantId(),
        zone.getName(),
        resourceId).getBytes());

    client.getKVClient().put(
        fromString(expiringKey),
        fromString("e-1")).join();

    verifyEtcdKeyExists(expiringKey, "e-1");

    // When a new active connection is seen it should remove any expiring key
    zoneWatchingService.handleActiveEnvoyConnection(zone, resourceId);

    verifyEtcdKeyDoesNotExist(expiringKey);

  }

  @Test
  public void testHandleActiveEnvoyDisconnection_privateZone() throws Exception {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    String tenantId = RandomStringUtils.randomAlphanumeric(10);
    String zoneName = RandomStringUtils.randomAlphanumeric(10).toLowerCase();
    String resourceId = RandomStringUtils.randomAlphanumeric(10);
    String envoyId = RandomStringUtils.randomAlphanumeric(10);
    long timeout = new Random().nextInt(1000) + 30L;

    final ResolvedZone zone = ResolvedZone.createPrivateZone(tenantId, zoneName);

    when(zoneApi.getByZoneName(anyString(), anyString()))
        .thenReturn(new ZoneDTO().setPollerTimeout(timeout));

    final long leaseId = grantLease(timeout);

    when(envoyLeaseTracking.grant(anyString(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(leaseId));

    zoneWatchingService.handleActiveEnvoyDisconnection(zone, resourceId, envoyId);

    String expiringKey = new String(EtcdUtils.buildKey(Keys.FMT_ZONE_EXPIRING,
        zone.getTenantId(),
        zone.getName(),
        resourceId).getBytes());

    GetResponse resp = verifyEtcdKeyExists(expiringKey.toLowerCase(), envoyId);

    long foundLeaseId = resp.getKvs().get(0).getLease();

    long ttl = client.getLeaseClient().timeToLive(foundLeaseId, LeaseOption.DEFAULT).get().getGrantedTTL();
    assertThat(ttl, equalTo(timeout));

    verify(zoneApi).getByZoneName(tenantId.toLowerCase(), zoneName);
  }

  @Test
  public void testHandleActiveEnvoyDisconnection_publicZone() throws Exception {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    String zoneName = ResolvedZone.PUBLIC_PREFIX + RandomStringUtils.randomAlphanumeric(10).toLowerCase();
    String resourceId = RandomStringUtils.randomAlphanumeric(10);
    String envoyId = RandomStringUtils.randomAlphanumeric(10);
    long timeout = new Random().nextInt(1000) + 30L;

    final ResolvedZone zone = ResolvedZone.createPublicZone(zoneName);

    when(zoneApi.getByZoneName(nullable(String.class), anyString()))
        .thenReturn(new ZoneDTO().setPollerTimeout(timeout));

    final long leaseId = grantLease(timeout);

    when(envoyLeaseTracking.grant(anyString(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(leaseId));

    zoneWatchingService.handleActiveEnvoyDisconnection(zone, resourceId, envoyId);

    String expiringKey = new String(EtcdUtils.buildKey(Keys.FMT_ZONE_EXPIRING,
        zone.getTenantForKey(),
        zone.getZoneNameForKey(),
        resourceId).getBytes());

    GetResponse resp = verifyEtcdKeyExists(expiringKey, envoyId);

    long foundLeaseId = resp.getKvs().get(0).getLease();

    long ttl = client.getLeaseClient().timeToLive(foundLeaseId, LeaseOption.DEFAULT).get().getGrantedTTL();
    assertThat(ttl, equalTo(timeout));

    verify(zoneApi).getByZoneName(null, zoneName);

    verify(zoneStorage).createExpiringEntry(zone, resourceId, envoyId, timeout);
  }

  @Test
  public void testHandleActiveEnvoyDisconnection_publicZone_failedPollerTimeoutRequest() throws Exception {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    String zoneName = ResolvedZone.PUBLIC_PREFIX + RandomStringUtils.randomAlphanumeric(10).toLowerCase();
    String resourceId = RandomStringUtils.randomAlphanumeric(10);
    String envoyId = RandomStringUtils.randomAlphanumeric(10);
    long timeout = new Random().nextInt(1000) + 30L;

    final ResolvedZone zone = ResolvedZone.createPublicZone(zoneName);

    when(zoneApi.getByZoneName(nullable(String.class), anyString()))
        .thenThrow(new IllegalArgumentException("bad request"));

    final long leaseId = grantLease(timeout);

    when(envoyLeaseTracking.grant(anyString(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(leaseId));

    zoneWatchingService.handleActiveEnvoyDisconnection(zone, resourceId, envoyId);

    String expiringKey = new String(EtcdUtils.buildKey(Keys.FMT_ZONE_EXPIRING,
        zone.getTenantForKey(),
        zone.getZoneNameForKey(),
        resourceId).getBytes());

    GetResponse resp = verifyEtcdKeyExists(expiringKey, envoyId);

    long foundLeaseId = resp.getKvs().get(0).getLease();

    long ttl = client.getLeaseClient().timeToLive(foundLeaseId, LeaseOption.DEFAULT).get().getGrantedTTL();
    assertThat(ttl, equalTo(timeout));

    verify(zoneApi).getByZoneName(null, zoneName);

    verify(zoneStorage).createExpiringEntry(zone, resourceId, envoyId, ZoneWatchingService.FALLBACK_POLLER_TIMEOUT);
  }

  @Test
  public void testHandleActiveEnvoyDisconnection_zoneNoLongerExists() throws Exception {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    String zoneName = ResolvedZone.PUBLIC_PREFIX + RandomStringUtils.randomAlphanumeric(10).toLowerCase();
    String resourceId = RandomStringUtils.randomAlphanumeric(10);
    String envoyId = RandomStringUtils.randomAlphanumeric(10);
    long timeout = new Random().nextInt(1000) + 30L;

    final ResolvedZone zone = ResolvedZone.createPublicZone(zoneName);

    when(zoneApi.getByZoneName(nullable(String.class), anyString()))
        .thenReturn(null);

    final long leaseId = grantLease(timeout);

    when(envoyLeaseTracking.grant(anyString(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(leaseId));

    zoneWatchingService.handleActiveEnvoyDisconnection(zone, resourceId, envoyId);

    String expiringKey = new String(EtcdUtils.buildKey(Keys.FMT_ZONE_EXPIRING,
        zone.getTenantForKey(),
        zone.getZoneNameForKey(),
        resourceId).getBytes());

    GetResponse resp = verifyEtcdKeyExists(expiringKey, envoyId);

    long foundLeaseId = resp.getKvs().get(0).getLease();

    long ttl = client.getLeaseClient().timeToLive(foundLeaseId, LeaseOption.DEFAULT).get().getGrantedTTL();
    assertThat(ttl, equalTo(timeout));

    verify(zoneApi).getByZoneName(null, zoneName);

    verify(zoneStorage).createExpiringEntry(zone, resourceId, envoyId, ZoneWatchingService.NULL_ZONE_POLLER_TIMEOUT);
  }

  @Test
  public void testHandleExpiredEnvoy() throws Exception {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, meterRegistry, topicProperties, zoneApi, etcdWatchConnector);

    String tenantId = RandomStringUtils.randomAlphanumeric(10);
    String zoneName = RandomStringUtils.randomAlphanumeric(10).toLowerCase();
    String resourceId = RandomStringUtils.randomAlphanumeric(10);
    String envoyId = RandomStringUtils.randomAlphanumeric(10);

    final ResolvedZone resolvedZone = ResolvedZone.createPrivateZone(tenantId, zoneName);

    registerAndWatchExpected(resolvedZone, resourceId.toLowerCase(), envoyId);

    String expectedKey = new String(EtcdUtils.buildKey(Keys.FMT_ZONE_EXPECTED,
        tenantId,
        zoneName,
        resourceId).getBytes());
    verifyEtcdKeyExists(expectedKey, envoyId);

    zoneWatchingService.handleExpiredEnvoy(resolvedZone, resourceId, envoyId);

    verifyEtcdKeyDoesNotExist(expectedKey);

    //noinspection unchecked
    verify(kafkaTemplate).send(
        eq("test.zones.json"),
        eq(String.format("%s:%s", tenantId.toLowerCase(), zoneName.toLowerCase())),
        eq(
            new ExpiredResourceZoneEvent()
                .setEnvoyId(envoyId)
                .setTenantId(tenantId.toLowerCase())
                .setZoneName(zoneName)
        )
    );

    verifyNoMoreInteractions(kafkaTemplate);

  }

  @Test
  public void testNewExpectedEnvoyResource() throws ExecutionException, InterruptedException {
    // This method handles all the new expected envoy actions but is also utilized to set up other
    // tests so it has been split out.
    registerAndWatchExpected(createPrivateZone("t-1", "z-1"), "r-1", "e-1");
  }

  /**
   * This test simulates the scenario where the zone management application is restarting, but
   * during the down time a new envoy-resource registered in the zone.
   */
  @Test
  public void testResumingExpectedEnvoyWatch() throws ExecutionException, InterruptedException {
    final String tenant = testName.getMethodName();

    final ResolvedZone resolvedZone = createPrivateZone(tenant, "z-1");

    registerAndWatchExpected(resolvedZone, "r-1", "e-1");

    // WORKAROUND until a release after 0.3.0 includes this change: https://github.com/etcd-io/jetcd/commit/d42722530d86579231905512c70fe3521532dcf3
    tearDown();
    setUp();

    // one envoy-resource has registered, now register another prior to re-watching

    final long leaseId = grantLease();
    zoneStorage.registerEnvoyInZone(resolvedZone, "e-2", "r-2", leaseId)
        .join();

    // sanity check KV content
    final GetResponse r2resp = client.getKVClient().get(
        fromString(new String(EtcdUtils.buildKey(Keys.FMT_ZONE_EXPECTED,
            tenant,
            "z-1",
            "r-2").getBytes()))
    ).get();
    assertThat(r2resp.getCount(), equalTo(1L));

    final GetResponse trackingResp = client.getKVClient().get(
        fromString("/tracking/zones/expected")
    ).get();
    assertThat(trackingResp.getCount(), equalTo(1L));
    // ...and the relative revisions of the tracking key vs the registration while not watching
    assertThat(trackingResp.getKvs().get(0).getModRevision(), lessThan(r2resp.getKvs().get(0).getModRevision()));

    // Now restart watching, which should pick up from where the other ended

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = etcdWatchConnector.watchExpectedZones(listener).get()) {

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone, "r-2");

    } finally {
      verifyNoMoreInteractions(listener);
    }

  }

  @Test
  public void testWatchExpectedZones_reRegister() throws ExecutionException, InterruptedException {
    final String tenant = testName.getMethodName();

    final ResolvedZone resolvedZone = createPrivateZone(tenant, "z-1");

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = etcdWatchConnector.watchExpectedZones(listener).get()) {

      final long leaseId = grantLease();

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-1", "r-1", leaseId)
          .join();

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone, "r-1");

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-2", "r-1", leaseId)
          .join();

      verify(listener, timeout(5000)).handleEnvoyResourceReassignedInZone(
          resolvedZone, "r-1", "e-1", "e-2");

    } finally {
      verifyNoMoreInteractions(listener);
    }

  }

  private void registerAndWatchExpected(ResolvedZone resolvedZone, String resourceId, String envoyId)
      throws InterruptedException, ExecutionException {

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = etcdWatchConnector.watchExpectedZones(listener).get()) {

      final long leaseId = grantLease();

      zoneStorage.registerEnvoyInZone(resolvedZone, envoyId, resourceId, leaseId)
          .join();

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone, resourceId);


    } finally {
      verifyNoMoreInteractions(listener);
    }
  }

  private GetResponse verifyEtcdKeyExists(String key, String value) throws Exception {
    GetResponse resp = client.getKVClient().get(
        fromString(key)).get();
    assertThat(resp.getKvs(), hasSize(1));
    if (value != null) {
      assertThat(resp.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8), equalTo(value));
    }
    return resp;
  }

  private void verifyEtcdKeyDoesNotExist(String key) throws Exception {
    GetResponse resp = client.getKVClient().get(
        fromString(key.toLowerCase())).get();

    assertThat(resp.getKvs(), hasSize(0));
  }

  private long grantLease() {
    return grantLease(10000);
  }

  private long grantLease(long ttl) {
    final LeaseGrantResponse leaseGrant = client.getLeaseClient().grant(ttl).join();
    return leaseGrant.getID();
  }
}