package com.linkedin.venice.benchmark;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.Time;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * To run the test, build the project and run the following commands:
 * ligradle jmh
 * If above command throws an error, you can try run `ligradle jmh --debug` first to clean up all the caches, then retry
 * `ligradle jmh` again to run the results.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
public class EventThrottlerBenchmark {
  private final int enforcementIntervalMS = 10 * Time.MS_PER_SECOND; // TokenBucket refill interval
  /**
   * Testing with different capacities: 1T (basically unlimited), 1M
   */
  @Param({ "1000000000000", "1000000" })
  protected long rcuPerSecond;

  /**
   * Testing with different tokensToConsume: 1 (single get), 100 (batch get)
   */
  @Param({ "1", "100" })
  protected long tokensToConsume;

  EventThrottler eventThrottler;

  AtomicLong approvedTotal = new AtomicLong(0);
  AtomicLong deniedTotal = new AtomicLong(0);

  @State(Scope.Thread)
  public static class ThreadContext {
    long approved;
    long denied;

    @TearDown
    public void end(EventThrottlerBenchmark benchmark) {
      benchmark.approvedTotal.addAndGet(approved);
      benchmark.deniedTotal.addAndGet(denied);
    }
  }

  /**
   * Copied from {@link com.linkedin.venice.listener.ReadQuotaEnforcementHandler}
   */
  private EventThrottler eventThrottlerFromRcuPerSecond(long totalRcuPerSecond) {
    return new EventThrottler(
        totalRcuPerSecond,
        enforcementIntervalMS,
        "test-throttler",
        true,
        EventThrottler.REJECT_STRATEGY);
  }

  @Setup
  public void setUp() {
    this.eventThrottler = eventThrottlerFromRcuPerSecond(rcuPerSecond);
  }

  @TearDown
  public void cleanUp() {
    long approved = approvedTotal.get();
    long denied = deniedTotal.get();
    double approvalRatio = (double) approved / ((double) approved + (double) denied);
    NumberFormat formatter = new DecimalFormat("#0.00");
    String approvalRatioStr = formatter.format(approvalRatio);

    System.out.println();
    System.out.println(
        "RCU/sec: " + rcuPerSecond + "; Tokens to consume: " + tokensToConsume + "; Approved: " + approved
            + "; Denied: " + denied + "; Approval ratio: " + approvalRatioStr);
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include(EventThrottlerBenchmark.class.getSimpleName())
            // .addProfiler(GCProfiler.class)
            .build();
    new Runner(opt).run();
  }

  @Benchmark
  @Threads(1)
  public void tokenBucketWithThreadCount1(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(2)
  public void tokenBucketWithThreadCount2(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(4)
  public void tokenBucketWithThreadCount4(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(8)
  public void tokenBucketWithThreadCount8(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  private void test(ThreadContext threadContext, Blackhole bh) {
    try {
      this.eventThrottler.maybeThrottle(tokensToConsume);
      bh.consume(threadContext.approved++);
    } catch (QuotaExceededException e) {
      bh.consume(threadContext.denied++);
    }
  }
}
