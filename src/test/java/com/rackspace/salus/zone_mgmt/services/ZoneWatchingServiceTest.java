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

import static com.rackspace.salus.telemetry.etcd.types.ResolvedZone.createPrivateZone;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.monitor_management.web.client.ZoneApi;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.telemetry.messaging.NewResourceZoneEvent;
import com.rackspace.salus.telemetry.messaging.ReattachedResourceZoneEvent;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
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

@RunWith(MockitoJUnitRunner.class)
public class ZoneWatchingServiceTest {

  @Rule
  public final EtcdClusterResource etcd = new EtcdClusterResource("ZoneWatchingTest", 1);

  @Rule
  public TestName testName = new TestName();

  @Mock
  KafkaTemplate kafkaTemplate;

  private ZoneStorage zoneStorage;

  private WatcherUtils watcherUtils;

  @Mock
  ZoneApi zoneApi;

  @Mock
  Watcher watcher;

  private Client client;

  @Before
  public void setUp() {
    final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
        .map(URI::toString)
        .collect(Collectors.toList());
    client = com.coreos.jetcd.Client.builder().endpoints(endpoints).build();

    zoneStorage = new ZoneStorage(client, null);

    watcherUtils = new WatcherUtils(zoneStorage);
  }

  @After
  public void tearDown() {
    zoneStorage.stop();
    client.close();
  }

  @Test
  public void testStart() {
    watcherUtils = Mockito.mock(WatcherUtils.class);
    when(watcherUtils.watchExpectedZones(any()))
        .thenReturn(CompletableFuture.completedFuture(watcher));
    when(watcherUtils.watchActiveZones(any()))
        .thenReturn(CompletableFuture.completedFuture(watcher));
    when(watcherUtils.watchExpiringZones((any())))
        .thenReturn(CompletableFuture.completedFuture(watcher));

    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, topicProperties, zoneApi, watcherUtils);

    zoneWatchingService.start();

    verify(watcherUtils).watchExpectedZones(same(zoneWatchingService));
    verify(watcherUtils).watchActiveZones(same(zoneWatchingService));
    verify(watcherUtils).watchExpiringZones(same(zoneWatchingService));
  }

  @Test
  public void handleNewEnvoyResourceInZone() {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, topicProperties, zoneApi, watcherUtils);

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
  public void handleEnvoyResourceReassignedInZone() {
    KafkaTopicProperties topicProperties = new KafkaTopicProperties();
    topicProperties.setZones("test.zones.json");
    final ZoneWatchingService zoneWatchingService = new ZoneWatchingService(
        zoneStorage, kafkaTemplate, topicProperties, zoneApi, watcherUtils);

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
  public void testNewExpectedEnvoyResource() throws ExecutionException, InterruptedException {
    registerAndWatchExpected(createPrivateZone("t-1", "z-1"), "r-1", "t-1");
  }

  /**
   * This test simulates the scenario where the zone management application is restarting, but
   * during the down time a new envoy-resource registered in the zone.
   */
  @Test
  public void testResumingExpectedEnvoyWatch() throws ExecutionException, InterruptedException {
    final String tenant = testName.getMethodName();

    final ResolvedZone resolvedZone = createPrivateZone(tenant, "z-1");

    registerAndWatchExpected(resolvedZone, "r-1", tenant);

    // one envoy-resource has registered, now register another prior to re-watching

    final long leaseId = grantLease();
    zoneStorage.registerEnvoyInZone(resolvedZone, "e-2", "r-2", leaseId)
        .join();

    // sanity check KV content
    final GetResponse r2resp = client.getKVClient().get(
        ByteSequence.fromString(String.format("/zones/expected/%s/z-1/r-2",
            tenant
        ))
    ).get();
    assertThat(r2resp.getCount(), equalTo(1L));

    final GetResponse trackingResp = client.getKVClient().get(
        ByteSequence.fromString("/tracking/zones/expected")
    ).get();
    assertThat(trackingResp.getCount(), equalTo(1L));
    // ...and the relative revisions of the tracking key vs the registration while not watching
    assertThat(trackingResp.getKvs().get(0).getModRevision(), lessThan(r2resp.getKvs().get(0).getModRevision()));

    // Now restart watching, which should pick up from where the other ended

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = watcherUtils.watchExpectedZones(listener).get()) {

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone, "r-2");

    } finally {
      // watcher has been closed

      verify(listener, timeout(5000)).handleExpectedZoneWatcherClosed(notNull());

      verifyNoMoreInteractions(listener);
    }

  }

  @Test
  public void testWatchExpectedZones_reRegister() throws ExecutionException, InterruptedException {
    final String tenant = testName.getMethodName();

    final ResolvedZone resolvedZone = createPrivateZone(tenant, "z-1");

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = watcherUtils.watchExpectedZones(listener).get()) {

      final long leaseId = grantLease();

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-1", "r-1", leaseId)
          .join();

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone, "r-1");

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-2", "r-1", leaseId)
          .join();

      verify(listener, timeout(5000)).handleEnvoyResourceReassignedInZone(
          resolvedZone, "r-1", "e-1", "e-2");

    } finally {
      // watcher has been closed

      // it is expected that watcher is closed with exception when closed prior to stopping the component
      verify(listener, timeout(5000)).handleExpectedZoneWatcherClosed(notNull());

      verifyNoMoreInteractions(listener);
    }

  }

  private void registerAndWatchExpected(ResolvedZone resolvedZone, String resourceId, String tenant)
      throws InterruptedException, ExecutionException {

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = watcherUtils.watchExpectedZones(listener).get()) {

      final long leaseId = grantLease();

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-1", resourceId, leaseId)
          .join();

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone, resourceId);


    } finally {
      // watcher has been closed due to it implementing AutoCloseable

      // it is expected that watcher is closed with exception when closed prior to stopping the component
      verify(listener, timeout(5000)).handleExpectedZoneWatcherClosed(notNull());

      verifyNoMoreInteractions(listener);
    }
  }

  private long grantLease() {
    return grantLease(10000);
  }

  private long grantLease(long ttl) {
    final LeaseGrantResponse leaseGrant = client.getLeaseClient().grant(ttl).join();
    final long leaseId = leaseGrant.getID();

    return leaseId;
  }
}