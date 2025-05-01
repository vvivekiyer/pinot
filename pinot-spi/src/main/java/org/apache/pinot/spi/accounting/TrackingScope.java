package org.apache.pinot.spi.accounting;
/**
 * Scope for tracking resources in ThreadResourceUsageAccountant.
 */
public enum TrackingScope {
  QUERY,
  WORKLOAD
}
