package org.littlestar.tpcc;

/**
 * A transaction type execution statistics in benchmark thread.
 * 
 * @author LiXiang
 *
 */
public class ExecutionStatistics {
	private volatile long totalRuntime = 0L;
	private volatile long maxRuntime = 0L;
	private volatile long maxRuntime2 = 0L;
	private volatile long succeedCount = 0L;
	private volatile long failureCount = 0L;
	private volatile long retryCount = 0L;

	public ExecutionStatistics() {}

	public ExecutionStatistics(long totalRuntime, long maxRuntime, long maxRuntime2, long succeedCount,
			long failureCount, long retryCount) {
		this.totalRuntime = totalRuntime;
		this.maxRuntime = maxRuntime;
		this.maxRuntime2 = maxRuntime2;
		this.succeedCount = succeedCount;
		this.failureCount = failureCount;
		this.retryCount = retryCount;
	}

	public long getRuntime() {
		return totalRuntime;
	}

	public void setTotalRuntime(long totalRuntime) {
		this.totalRuntime = totalRuntime;
	}

	public long getMaxRuntime() {
		return maxRuntime;
	}

	public void setMaxRuntime(long maxRuntime) {
		this.maxRuntime = Math.max(this.maxRuntime, maxRuntime);
	}

	public long getMaxRuntime2() {
		return maxRuntime2;
	}

	public void setMaxRuntime2(long maxRuntime2) {
		this.maxRuntime2 = Math.max(this.maxRuntime2, maxRuntime2);
	}

	public long getSucceedCount() {
		return succeedCount;
	}

	public void setSucceedCount(long succeedCount) {
		this.succeedCount = succeedCount;
	}

	public long getFailureCount() {
		return failureCount;
	}

	public void setFailureCount(long failureCount) {
		this.failureCount = failureCount;
	}

	public long getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(long retryCount) {
		this.retryCount = retryCount;
	}
}