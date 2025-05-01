package org.apache.pinot.core.accounting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkloadAggregator implements ResourceAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadAggregator.class);

  private final boolean _isThreadCPUSamplingEnabled;
  private final boolean _isThreadMemorySamplingEnabled;
  private final PinotConfiguration _config;
  private final InstanceType _instanceType;
  private final String _instanceId;

  // For one time concurrent update of stats. This is to provide stats collection for parts that are not
  // performance sensitive and workload_name is not known earlier (eg: broker inbound netty request)
  private final ConcurrentHashMap<String, Long> _concurrentTaskCPUStatsAggregator = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> _concurrentTaskMemStatsAggregator = new ConcurrentHashMap<>();

  private final int _sleepTimeMs;
  private final boolean _enableEnforcement;

  WorkloadBudgetManager _workloadBudgetManager;

  // Aggregation state
  Map<String, List<CPUMemThreadLevelAccountingObjects.TaskEntry>> allWorkloads = new HashMap<>();
  HashMap<String, Long> finishedTaskCPUCost = new HashMap<>();
  HashMap<String, Long> finishedTaskMemCost = new HashMap<>();


  public WorkloadAggregator(boolean isThreadCPUSamplingEnabled, boolean isThreadMemSamplingEnabled,
      PinotConfiguration config, InstanceType instanceType, String instanceId) {
    _isThreadCPUSamplingEnabled = isThreadCPUSamplingEnabled;
    _isThreadMemorySamplingEnabled = isThreadMemSamplingEnabled;
    _config = config;
    _instanceType = instanceType;
    _instanceId = instanceId;

    _workloadBudgetManager = WorkloadBudgetManager.getInstance();
    _sleepTimeMs = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_SLEEP_TIME_MS);
    _enableEnforcement = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT);

    LOGGER.info("WorkloadAggregator initialized with isThreadCPUSamplingEnabled: {}, isThreadMemorySamplingEnabled: {}",
        _isThreadCPUSamplingEnabled, _isThreadMemorySamplingEnabled);
  }


  @Override
  public void updateConcurrentCpuUsage(String workload, long cpuUsageNS) {
    _concurrentTaskCPUStatsAggregator.compute(workload,
        (key, value) -> (value == null) ? cpuUsageNS : (value + cpuUsageNS));
  }

  @Override
  public void updateConcurrentMemUsage(String workload, long memBytes) {
    _concurrentTaskMemStatsAggregator.compute(workload,
        (key, value) -> (value == null) ? memBytes : (value + memBytes));
  }

  @Override
  public void cleanUpPostAggregation() {
    // TODO(Vivek): This is a no-op for now. Validate.
    // No-op.
  }

  @Override
  public int getAggregationSleepTimeMs() {
    return _sleepTimeMs;
  }

  @Override
  public void preAggregate(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries) {
    allWorkloads.clear();
    finishedTaskCPUCost.clear();
    finishedTaskMemCost.clear();
  }

  @Override
  public void aggregate(Thread thread, CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry) {
    CPUMemThreadLevelAccountingObjects.TaskEntry currentQueryTask = threadEntry.getCurrentThreadTaskStatus();
    if (currentQueryTask == null) {
      // This means that task has finished or the task doesn't have a workload.
      // TODO(Vivek): Check if this model of doing a busy aggregation is enough.
      return;
    }

    String currentTaskWorkload = currentQueryTask.getWorkloadName();
    long currentCPUCost = threadEntry._currentThreadCPUTimeSampleMS;
    long currentMemoryCost = threadEntry._currentThreadMemoryAllocationSampleBytes;

    CPUMemThreadLevelAccountingObjects.TaskEntry prevQueryTask = threadEntry._previousThreadTaskStatus;

    // The cost to charge against the workloadBudgetManager.
    long deltaCPUCost = currentCPUCost;
    long deltaMemCost = currentMemoryCost;

    if (prevQueryTask != null) {
      if (currentQueryTask == prevQueryTask) {
        deltaCPUCost = currentCPUCost - threadEntry._previousThreadCPUTimeSampleMS;
        deltaMemCost = currentMemoryCost - threadEntry._previousThreadMemoryAllocationSampleBytes;
      }
    }

    finishedTaskCPUCost.merge(currentTaskWorkload, deltaCPUCost, Long::sum);
    finishedTaskMemCost.merge(currentTaskWorkload, deltaMemCost, Long::sum);

    if (!allWorkloads.containsKey(currentTaskWorkload)) {
      allWorkloads.put(currentTaskWorkload, new ArrayList<>());
    }
    allWorkloads.get(currentTaskWorkload).add(currentQueryTask);
  }

  @Override
  public void postAggregate() {
    for (Map.Entry<String, List<CPUMemThreadLevelAccountingObjects.TaskEntry>> workloadEntry : allWorkloads.entrySet()) {
      String workloadName = workloadEntry.getKey();
      List<CPUMemThreadLevelAccountingObjects.TaskEntry> taskEntries = workloadEntry.getValue();
      long finishedCPUCost = finishedTaskCPUCost.getOrDefault(workloadName, 0L);
      long finishedMemCost = finishedTaskMemCost.getOrDefault(workloadName, 0L);

      WorkloadBudgetManager.BudgetStats budgetStats =
          _workloadBudgetManager.tryCharge(workloadName, finishedCPUCost, finishedMemCost);
      LOGGER.debug("Workload: {}. Remaining budget CPU: {}, Memory: {}", workloadName, budgetStats.cpuRemaining,
          budgetStats.memoryRemaining);

      if (!_enableEnforcement) {
        // Nothing to do.
        LOGGER.debug("Workload Cost Enforcement is disabled. Skipping enforcement for workload: {}", workloadName);
        continue;
      }

      if (budgetStats.cpuRemaining <= 0 || budgetStats.memoryRemaining <= 0) {
        // Interrupt all queries in the list
        for (CPUMemThreadLevelAccountingObjects.TaskEntry taskEntry : taskEntries) {
          String queryId = taskEntry.getQueryId();
          Thread anchorThread = taskEntry.getAnchorThread();
          if (!anchorThread.isInterrupted()) {
            LOGGER.info("Killing query: {} and anchorThread:{}, Remaining budget CPU: {}, Memory: {}", queryId,
                anchorThread.getName(), budgetStats.cpuRemaining, budgetStats.memoryRemaining);
            anchorThread.interrupt();
          }
        }
      }
    }
  }
}
