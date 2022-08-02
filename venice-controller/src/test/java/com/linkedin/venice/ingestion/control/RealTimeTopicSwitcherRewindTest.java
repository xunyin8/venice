package com.linkedin.venice.ingestion.control;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class RealTimeTopicSwitcherRewindTest {
  private RealTimeTopicSwitcher topicReplicator;
  private MockTime mockTime;

  @BeforeTest
  public void setUp() {
    final Map<Integer, Long> startingOffsets = new HashMap<>();
    startingOffsets.put(0, 10L);
    startingOffsets.put(1, 20L);
    startingOffsets.put(2, 15L);
    final List<Long> startingOffsetsList = new ArrayList<>();
    startingOffsets.forEach((integer, aLong) -> startingOffsetsList.add(integer, aLong));

    TopicManager topicManager = mock(TopicManager.class);
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = mock(VeniceWriter.class);
    topicReplicator = mock(RealTimeTopicSwitcher.class);
    VeniceWriterFactory veniceWriterFactory = mock(VeniceWriterFactory.class);
    mockTime = new MockTime();

    when(topicReplicator.getTopicManager()).thenReturn(topicManager);
    when(topicReplicator.getVeniceWriterFactory()).thenReturn(veniceWriterFactory);
    when(topicReplicator.getTimer()).thenReturn(mockTime);
    when(topicManager.containsTopicAndAllPartitionsAreOnline(anyString())).thenReturn(true);
    when(veniceWriterFactory.createBasicVeniceWriter(
        anyString(),
        any(Time.class)))
        .thenReturn(veniceWriter);

    // Methods under test
    doCallRealMethod().when(topicReplicator).ensurePreconditions(anyString(), anyString(), any(), any());
    doCallRealMethod().when(topicReplicator).getRewindStartTime(any(), any(), anyLong());
    doCallRealMethod().when(topicReplicator).sendTopicSwitch(anyString(), anyString(), anyLong(), anyList());
  }

  @Test
  public void testStartBufferReplayRewindFromEOP() throws TopicException {
    final Store store = TestUtils.createTestStore(Utils.getUniqueString("store"), "owner", 1);
    final long REWIND_TIME_IN_SECONDS = 5;
    final long VERSION_CREATION_TIME_MS = 15000;
    Optional<HybridStoreConfig> hybridStoreConfig  = Optional.of((new HybridStoreConfigImpl(REWIND_TIME_IN_SECONDS, 1,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD, DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP)));
    final String sourceTopicName = "source topic name";
    final String destinationTopicName = "destination topic name";

    topicReplicator.ensurePreconditions(sourceTopicName, destinationTopicName, store, hybridStoreConfig);
    long rewindStartTime = topicReplicator.getRewindStartTime(mock(Version.class), hybridStoreConfig, VERSION_CREATION_TIME_MS);
    assertEquals(rewindStartTime, mockTime.getMilliseconds() - Time.MS_PER_SECOND * REWIND_TIME_IN_SECONDS,
        "Rewind start timestamp is not calculated properly");
    topicReplicator.sendTopicSwitch(sourceTopicName, destinationTopicName, rewindStartTime, null);

    verify(topicReplicator).sendTopicSwitch(sourceTopicName, destinationTopicName, rewindStartTime, null);

    try {
      topicReplicator.ensurePreconditions(sourceTopicName, destinationTopicName, store, Optional.empty());
      fail("topicReplicator.startBufferReplay should fail (FOR NOW) for non-Hybrid stores.");
    } catch (VeniceException e) {
      // expected
    }
  }

  @Test
  public void testStartBufferRewindFromSOP() throws TopicException {
    final Store store = TestUtils.createTestStore(Utils.getUniqueString("store"), "owner", 1);
    final long REWIND_TIME_IN_SECONDS = 5;
    final long VERSION_CREATION_TIME_MS = 15000;
    Optional<HybridStoreConfig> hybridStoreConfig  = Optional.of((new HybridStoreConfigImpl(REWIND_TIME_IN_SECONDS, 1,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD, DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_SOP)));
    final String sourceTopicName = "source topic name";
    final String destinationTopicName = "destination topic name";

    topicReplicator.ensurePreconditions(sourceTopicName, destinationTopicName, store, hybridStoreConfig);
    long rewindStartTime = topicReplicator.getRewindStartTime(mock(Version.class), hybridStoreConfig, VERSION_CREATION_TIME_MS);
    assertEquals(rewindStartTime, VERSION_CREATION_TIME_MS - Time.MS_PER_SECOND * REWIND_TIME_IN_SECONDS, "Rewind start timestamp is not calculated properly");
    topicReplicator.sendTopicSwitch(sourceTopicName, destinationTopicName, rewindStartTime, null);

    verify(topicReplicator).sendTopicSwitch(sourceTopicName, destinationTopicName, rewindStartTime, null);

    try {
      topicReplicator.ensurePreconditions(sourceTopicName, destinationTopicName, store, Optional.empty());
      fail("topicReplicator.startBufferReplay should fail (FOR NOW) for non-Hybrid stores.");
    } catch (VeniceException e) {
      // expected
    }
  }
}