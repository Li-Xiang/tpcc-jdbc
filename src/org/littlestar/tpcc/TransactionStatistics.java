package org.littlestar.tpcc;

public class TransactionStatistics {
	private boolean isSuccess = false;
	private long elapsedTime = 0L;
	private int retryCount = 0;
	private TransactionType type;

	public TransactionStatistics(TransactionType type, boolean isSuccess, long elapsedTime, int retryCount) {
		this.isSuccess = isSuccess;
		this.elapsedTime = elapsedTime;
		this.retryCount = retryCount;
		this.type = type;
	}

	public void setElapsedTime(long elapsed) {
		elapsedTime = elapsed;
	}

	public long getElapsedTime() {
		return elapsedTime;
	}

	public void setRetryCount(int count) {
		retryCount = count;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void success(boolean status) {
		isSuccess = status;
	}

	public boolean isSuccess() {
		return isSuccess;
	}

	public TransactionType getTransactionType() {
		return type;
	}

	@Override
	public String toString() {
		return "transaction = " + type.toString() + "; success = " + isSuccess + "; elapsed = " + elapsedTime
				+ " ms; retry = " + retryCount;
	}
}