package org.apache.pinot.core.query.scheduler;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.fcfs.FCFSSchedulerGroup;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.scheduler.resources.WorkloadAwareResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkloadAwareScheduler extends QueryScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadAwareScheduler.class);

  private final int _numNonProdRunners;
  private final Semaphore _nonProdRunnerSemaphore;

  Thread _scheduler;

  private final MultiLevelPriorityQueue _queryQueue;

  public static WorkloadAwareScheduler create(PinotConfiguration config, QueryExecutor queryExecutor,
      ServerMetrics metrics, LongAccumulator latestQueryTime) {
    // Create new resource manager
    final ResourceManager rm = new WorkloadAwareResourceManager(config);
    return new WorkloadAwareScheduler(config, queryExecutor, rm, metrics, latestQueryTime);
  }

  private WorkloadAwareScheduler(PinotConfiguration config, QueryExecutor queryExecutor,
      ResourceManager resourceManager, ServerMetrics metrics, LongAccumulator latestQueryTime) {
    super(config, queryExecutor, resourceManager, metrics, latestQueryTime);

    final SchedulerGroupFactory groupFactory = new SchedulerGroupFactory() {
      @Override
      public SchedulerGroup create(PinotConfiguration config, String groupName) {
        return new FCFSSchedulerGroup(groupName);
      }
    };
    _queryQueue = new MultiLevelPriorityQueue(config, resourceManager, groupFactory, new WorkloadGroupMapper());

    // TODO(Vivek): Make the semaphore number based on a config.
    _numNonProdRunners = 4;
    _nonProdRunnerSemaphore = new Semaphore(_numNonProdRunners);
  }


  @Override
  public String name() {
    return "WorkloadAwareScheduler";
  }

  @Override
  public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
    if (!_isRunning) {
      return immediateErrorResponse(queryRequest, QueryException.SERVER_SCHEDULER_DOWN_ERROR);
    }
    queryRequest.getTimerContext().startNewPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    if (!isNonProdQuery(queryRequest)) {
      QueryExecutorService queryExecutorService = _resourceManager.getExecutorService(queryRequest, null);
      ListenableFutureTask<byte[]> queryTask = createQueryFutureTask(queryRequest, queryExecutorService);
      _resourceManager.getQueryRunners().submit(queryTask);
      return queryTask;
    }

    final SchedulerQueryContext schedQueryContext = new SchedulerQueryContext(queryRequest);
    try {
      _queryQueue.put(schedQueryContext);
    } catch (OutOfCapacityException e) {
      LOGGER.error("Out of capacity for table {}, message: {}", queryRequest.getTableNameWithType(), e.getMessage());
      return immediateErrorResponse(queryRequest, QueryException.SERVER_OUT_OF_CAPACITY_ERROR);
    }
    return schedQueryContext.getResultFuture();
  }

  @Override
  public void start() {
    super.start();
    _scheduler = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_isRunning) {
          try {
            _nonProdRunnerSemaphore.acquire();
          } catch (InterruptedException e) {
            if (!_isRunning) {
              LOGGER.info("Shutting down scheduler");
            } else {
              LOGGER.error("Interrupt while acquiring semaphore. Exiting.", e);
            }
            break;
          }
          try {
            final SchedulerQueryContext request = _queryQueue.take();
            if (request == null) {
              continue;
            }
            ServerQueryRequest queryRequest = request.getQueryRequest();
            final QueryExecutorService executor =
                _resourceManager.getExecutorService(queryRequest, request.getSchedulerGroup());
            final ListenableFutureTask<byte[]> queryFutureTask = createQueryFutureTask(queryRequest, executor);
            queryFutureTask.addListener(new Runnable() {
              @Override
              public void run() {
                executor.releaseWorkers();
                request.getSchedulerGroup().endQuery();
                _nonProdRunnerSemaphore.release();
                checkStopResourceManager();
                if (!_isRunning && _nonProdRunnerSemaphore.availablePermits() == _numNonProdRunners) {
                  _resourceManager.stop();
                }
              }
            }, MoreExecutors.directExecutor());
            request.setResultFuture(queryFutureTask);
            request.getSchedulerGroup().startQuery();
            queryRequest.getTimerContext().getPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT).stopAndRecord();
            _resourceManager.getQueryRunners().submit(queryFutureTask);
          } catch (Throwable t) {
            LOGGER.error(
                "Error in scheduler thread. This is indicative of a bug. Please report this. Server will continue "
                    + "with errors", t);
          }
        }
        if (_isRunning) {
          throw new RuntimeException("FATAL: Scheduler thread is quitting.....something went horribly wrong.....!!!");
        } else {
          failAllPendingQueries();
        }
      }
    });
    _scheduler.setName("scheduler");
    _scheduler.setPriority(Thread.MAX_PRIORITY);
    _scheduler.setDaemon(true);
    _scheduler.start();
  }

  @Override
  public void stop() {
    super.stop();
    // without this, scheduler will never stop if there are no pending queries
    if (_scheduler != null) {
      _scheduler.interrupt();
    }
  }

  private boolean isNonProdQuery(ServerQueryRequest queryRequest) {
    QueryContext queryContext = queryRequest.getQueryContext();
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    return Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.IS_NON_PROD_QUERY));
  }

  private void checkStopResourceManager() {
    if (!_isRunning && _nonProdRunnerSemaphore.availablePermits() == _numNonProdRunners) {
      _resourceManager.stop();
    }
  }

  synchronized private void failAllPendingQueries() {
    List<SchedulerQueryContext> pending = _queryQueue.drain();
    for (SchedulerQueryContext queryContext : pending) {
      queryContext.setResultFuture(
          immediateErrorResponse(queryContext.getQueryRequest(), QueryException.SERVER_SCHEDULER_DOWN_ERROR));
    }
  }
}
