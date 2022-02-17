package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.controllerapi.ControllerTransport;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;

import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.controllerapi.ControllerRoute.REQUEST_TOPIC;
import static com.linkedin.venice.controllerapi.ControllerRoute.JOB;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

public class TestBackupControllerResponse {
  @Test
  public void backupControllerThrows421() throws Exception {
    String clusterName = "backupControllerThrows421";
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker(zkServer);
        ControllerTransport transport = new ControllerTransport(Optional.empty());
        VeniceControllerWrapper controller1 = ServiceFactory.getVeniceController(clusterName, kafka);
        VeniceControllerWrapper controller2 = ServiceFactory.getVeniceController(clusterName, kafka)) {
      // TODO: Eliminate sleep to make test reliable
      Thread.sleep(2000);
      VeniceControllerWrapper nonMasterController = !controller1.isMasterController(clusterName) ? controller1 : controller2;
      try {
        transport.request(
            nonMasterController.getControllerUrl(),
            REQUEST_TOPIC,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
      } catch (VeniceHttpException e) {
        Assert.assertEquals(e.getHttpStatusCode(), HttpConstants.SC_MISDIRECTED_REQUEST);
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      try {
        transport.request(
            nonMasterController.getControllerUrl(),
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
      } catch (VeniceHttpException e) {
        Assert.assertEquals(e.getHttpStatusCode(), HttpConstants.SC_MISDIRECTED_REQUEST);
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      try {
        int timeoutMs = 1;
        transport.request(
            controller1.getControllerUrl(),
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class,
            timeoutMs, null);
        Assert.fail("Expected TimeoutException did not happen");
      } catch (TimeoutException e) {
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      try {
        String invalidControllerUrl = "http://0.0.0.0";
        transport.request(
            invalidControllerUrl,
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
        Assert.fail("Expected ExecutionException did not happen");
      } catch (ExecutionException e) {
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }


      String deadControllerUrl = controller1.getControllerUrl();
      controller1.close();
      controller2.close();
      try {
        transport.request(
            deadControllerUrl,
            JOB,
            new QueryParams().add(CLUSTER, clusterName),
            VersionCreationResponse.class);
        Assert.fail("Expected ExecutionException did not happen");
      } catch (ExecutionException e) {
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }
    }
  }
}
