package org.apache.pinot.core.accounting;

import java.util.List;


public interface ResourceAggregator {

  public void updateConcurrentCpuUsage(String name, long cpuTimeNs);

  public void updateConcurrentMemUsage(String name, long memBytes);

  public void cleanUpPostAggregation();

  public int getAggregationSleepTimeMs();

  public void preAggregate(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries);

  public void aggregate(Thread thread, CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry);

  public void postAggregate();
}
