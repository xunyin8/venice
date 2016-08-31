package com.linkedin.venice.helix;

import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.status.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixStatusMessageChannel
 */
public class TestHelixStatusMessageChannel {
  private String cluster = "UnitTestCluster";
  private String kafkaTopic = "test_resource_1";
  private int partitionId = 0;
  private String instanceId = "localhost_1234";
  private ExecutionStatus status = ExecutionStatus.COMPLETED;
  private ZkServerWrapper zkServerWrapper;
  private String zkAddress;
  private HelixStatusMessageChannel channel;
  private HelixManager manager;
  private HelixAdmin admin;
  private HelixManager controller;
  private final long WAIT_ZK_TIME = 1000l;

  @BeforeMethod
  public void setup()
      throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(cluster);
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(cluster).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(cluster, TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
        TestHelixRoutingDataRepository.UnitTestStateModel.getDefinition());

    admin.addResource(cluster, kafkaTopic, 1, TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(cluster, kafkaTopic, 1);

    controller = HelixControllerMain
        .startHelixController(zkAddress, cluster, "UnitTestController", HelixControllerMain.STANDALONE);
    controller.connect();

    manager = HelixManagerFactory.getZKHelixManager(cluster, instanceId, InstanceType.PARTICIPANT, zkAddress);
    manager.getStateMachineEngine()
        .registerStateModelFactory(TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
            new TestHelixRoutingDataRepository.UnitTestStateModelFactory());

    manager.connect();
    channel = new HelixStatusMessageChannel(manager);
  }

  @AfterMethod
  public void cleanup() {
    manager.disconnect();
    controller.disconnect();
    admin.dropCluster(cluster);
    admin.close();
    zkServerWrapper.close();
  }

  private void compareConversion(StoreStatusMessage veniceMessage) {
    Message helixMessage = channel.convertVeniceMessageToHelixMessage(veniceMessage);
    Assert.assertEquals(veniceMessage.getMessageId(), helixMessage.getMsgId(),
            "Message Ids are different.");
    Assert.assertEquals(StoreStatusMessage.class.getName(),
            helixMessage.getRecord().getSimpleField(HelixStatusMessageChannel.VENICE_MESSAGE_CLASS),
            "Class names are different.");
    Map<String, String> fields = helixMessage.getRecord().getMapField(HelixStatusMessageChannel.VENICE_MESSAGE_FIELD);
    for (Map.Entry<String, String> entry : veniceMessage.getFields().entrySet()) {
      Assert.assertEquals(entry.getValue(), fields.get(entry.getKey()),
              "Message fields are different.");
    }

    StoreStatusMessage convertedVeniceMessage = (StoreStatusMessage) channel.convertHelixMessageToVeniceMessage(helixMessage);
    Assert.assertEquals(veniceMessage, convertedVeniceMessage,
            "Message fields are different. Convert it failed,");
  }

  @Test
  public void testConvertBetweenVeniceMessageAndHelixMessage()
      throws ClassNotFoundException {
    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    compareConversion(veniceMessage);

    veniceMessage.setOffset(10);
    compareConversion(veniceMessage);

    veniceMessage.setDescription("Sample Description ");
    compareConversion(veniceMessage);
  }

  @Test
  public void testRegisterHandler() {
    StoreStatusMessageHandler hander = new StoreStatusMessageHandler();

    channel.registerHandler(StoreStatusMessage.class, hander);

    Assert.assertEquals(hander, channel.getHandler(StoreStatusMessage.class),
        "Can not get correct handler.Register is failed.");

    channel.unRegisterHandler(StoreStatusMessage.class, hander);
    try {
      channel.getHandler(StoreStatusMessage.class);
      Assert.fail("Handler should be un-register before.");
    } catch (VeniceException e) {
      //Expected.
    }
  }

  @Test
  public void testSendMessage()
      throws IOException, InterruptedException {
    //Register handler for message in controler side.
    StoreStatusMessageHandler handler = new StoreStatusMessageHandler();
    HelixStatusMessageChannel controllerChannel = getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage);
    StoreStatusMessage receivedMessage = handler.getStatus(veniceMessage.getKafkaTopic());
    Assert.assertNotNull(receivedMessage, "Message is not received.");
    Assert.assertEquals(veniceMessage.getMessageId(), receivedMessage.getMessageId(),
        "Message is not received correctly. Id is wrong.");

    Assert.assertEquals(veniceMessage.getFields(), receivedMessage.getFields(),
        "Message is not received correctly. Fields are wrong");
  }

  private HelixStatusMessageChannel getControllerChannel(StatusMessageHandler<StoreStatusMessage> handler) {
    HelixStatusMessageChannel controllerChannel = new HelixStatusMessageChannel(controller);
    controllerChannel.registerHandler(StoreStatusMessage.class, handler);
    return controllerChannel;
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSendMessageFailed()
      throws IOException, InterruptedException {
    int retryCount = 1;
    FailedTestStoreStatusMessageHandler handler = new FailedTestStoreStatusMessageHandler(retryCount);
    HelixStatusMessageChannel controllerChannel = getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage, 0, 0);
    Assert.fail("Sending should be failed, because we thrown an exception during handing message.");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSendMessageRetryFailed()
      throws IOException, InterruptedException {
    int retryCount = 5;
    FailedTestStoreStatusMessageHandler handler = new FailedTestStoreStatusMessageHandler(retryCount);
    HelixStatusMessageChannel controllerChannel = getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage, 2, 0);
    Assert.fail("Sending should be failed, because after retrying 2 times, handling message is stil failed.");
  }

  @Test
  public void testSendMessageRetrySuccessful()
      throws IOException, InterruptedException {
    int retryCount = 2;
    FailedTestStoreStatusMessageHandler handler = new FailedTestStoreStatusMessageHandler(retryCount);
    HelixStatusMessageChannel controllerChannel = getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      channel.sendToController(veniceMessage, retryCount, 0);
    } catch (VeniceException e) {
      Assert.fail("Sending should be successful after retrying " + retryCount + " times", e);
    }
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSendMessageTimeout()
      throws IOException, InterruptedException {
    int timeoutCount = 1;
    TimeoutTestStoreStatusMessageHandler handler = new TimeoutTestStoreStatusMessageHandler(timeoutCount);
    HelixStatusMessageChannel controllerChannel = getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    channel.sendToController(veniceMessage, 0, 0);
    Assert.fail("Sending should be failed, because timeout");
  }

  @Test
  public void testSendMessageHandleTimeout()
      throws IOException, InterruptedException {
    int timeoutCount = 1;
    TimeoutTestStoreStatusMessageHandler handler = new TimeoutTestStoreStatusMessageHandler(timeoutCount);
    HelixStatusMessageChannel controllerChannel = getControllerChannel(handler);

    StoreStatusMessage veniceMessage = new StoreStatusMessage(kafkaTopic, partitionId, instanceId, status);
    try {
      channel.sendToController(veniceMessage, timeoutCount, 0);
    } catch (VeniceException e) {
      Assert.fail("Sending should be successful after retry " + timeoutCount + " times", e);
    }
  }

  /**
   * Handler in controller side used to deal with status update message from storage node.
   */
  private static class FailedTestStoreStatusMessageHandler implements StatusMessageHandler<StoreStatusMessage> {
    private int errorReplyCount;
    private int errorReply = 0;

    public FailedTestStoreStatusMessageHandler(int errorReplyCount) {
      this.errorReplyCount = errorReplyCount;
    }

    @Override
    public void handleMessage(StoreStatusMessage message) {
      if (errorReply == errorReplyCount) {
        //handle message correctly.
      } else {
        errorReply++;
        throw new VeniceException("Failed to handle message." + " ErrorReply #" + errorReply);
      }
    }
  }

  /**
   * Handler in controller side used to deal with status update message from storage node.
   */
  private static class TimeoutTestStoreStatusMessageHandler implements StatusMessageHandler<StoreStatusMessage> {
    private int timeOutReplyCount;
    private int timeOutReply = 0;

    public TimeoutTestStoreStatusMessageHandler(int timeOutReplyCount) {
      this.timeOutReplyCount = timeOutReplyCount;
    }

    @Override
    public void handleMessage(StoreStatusMessage message) {
      if (timeOutReply == timeOutReplyCount) {
        //handle message correctly.
      } else {
        timeOutReply++;
        try {
          Thread.sleep(HelixStatusMessageChannel.WAIT_TIME_OUT + 300);
        } catch (InterruptedException e) {
        }
      }
    }
  }
}
