package com.rackspace.salus.zw.web.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@WebMvcTest(ZoneApiController.class)
public class ZoneApiControllerTest {

  @Autowired
  MockMvc mvc;

  @MockBean
  ZoneStorage zoneStorage;

  @Test
  public void testGetExpiredPollerPublicZone() throws Exception {

    when(zoneStorage.getExpiredPollerResourceIdsInZone(any()))
        .thenReturn(CompletableFuture.completedFuture(List.of("r-1", "r-2")));

    ResolvedZone resolvedZone = ResolvedZone.createPublicZone("public/testPublicZone");
    final MvcResult result = mvc.perform(
        get("/api/admin/zone/{name}",
            "public/testPublicZone"
        )
    )
        .andExpect(request().asyncStarted())
        .andReturn();

    mvc.perform(asyncDispatch(result))
        .andExpect(status().isOk())
        .andExpect(jsonPath("*").value(Matchers.containsInAnyOrder("r-1", "r-2")));

    verify(zoneStorage).getExpiredPollerResourceIdsInZone(resolvedZone);
  }

  @Test
  public void testGetExpiredPollerPrivateZone() throws Exception {

    when(zoneStorage.getExpiredPollerResourceIdsInZone(any()))
        .thenReturn(CompletableFuture.completedFuture(List.of("r-1", "r-2")));

    ResolvedZone resolvedZone = ResolvedZone.createPrivateZone("t-1", "testZone");

    final MvcResult result = mvc.perform(
        get("/api/tenant/{tenantId}/zone/{name}/detached-pollers",
            "t-1", "testZone"
        )
    )
        .andExpect(request().asyncStarted())
        .andReturn();

    mvc.perform(asyncDispatch(result))
        .andExpect(status().isOk())
        .andExpect(jsonPath("*").value(Matchers.containsInAnyOrder("r-1", "r-2")));

    verify(zoneStorage).getExpiredPollerResourceIdsInZone(resolvedZone);
  }
}
