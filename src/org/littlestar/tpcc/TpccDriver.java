package org.littlestar.tpcc;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.littlestar.tpcc.datasource.TpccDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TpccDriver implements TpccConstants {
	private final static Logger LOGGER = LoggerFactory.getLogger(TpccDriver.class);
	private final TpccDataSource dataSource;
	private final Dbms dbms;
	private int wareCount;
	
	public TpccDriver(TpccDataSource ds, Dbms dbms, int wareCount) {
		this.dataSource = ds;
		this.dbms = dbms;
		this.wareCount = wareCount;
		
	}
	
	public static AtomicBoolean transactionOn = new AtomicBoolean(true);
	public static AtomicBoolean countingOn = new AtomicBoolean(false);
	private LocalDateTime benchCountingEndTime;
	public void benchmark(int runTime, int rampUp, int reportPeriod, int threads) throws Exception {
		ThreadFactory benchmarkFactory = new ThreadFactoryBuilder().setNameFormat("tpcc-benchmark-pool-%d").build();
		ThreadPoolExecutor benchmarkExecutor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(threads), benchmarkFactory, new ThreadPoolExecutor.AbortPolicy());

		ThreadFactory monitorFactory = new ThreadFactoryBuilder().setNameFormat("benchmark-monitor-pool-%d").build();
		ScheduledExecutorService monitorExecutor = new ScheduledThreadPoolExecutor(2, monitorFactory);

		////// 创建并启动测试线程。
		LinkedList<BenchmarkThread> benchmarkThreads = new LinkedList<BenchmarkThread>();
		for (int i = 0; i < threads; i++) {
			BenchmarkThread benchmarkThread = new BenchmarkThread();
			benchmarkThreads.add(benchmarkThread);
			benchmarkExecutor.submit(benchmarkThread);
		}
		
		BenchmarkReporter.reportWelcome(dataSource, wareCount, threads);
		////// 热身, 延迟开启事务计数器标志.
		if (rampUp > 0) {
			LOGGER.info("Ramp-up " + rampUp + " sec... ");
			TimeUnit.SECONDS.sleep(rampUp);
		}
		
		countingOn.set(true); // 开启事务计数器
		LocalDateTime benchCountingBeginTime = LocalDateTime.now();
		////// 2 scheduled threads: one for interval output, other one for stop benchmark threads when run-time reached.
		monitorExecutor.schedule(() -> {
			transactionOn.set(false);
			countingOn.set(false);
			benchmarkExecutor.shutdown();
			monitorExecutor.shutdown();
			benchCountingEndTime = LocalDateTime.now();
		}, runTime, TimeUnit.SECONDS);
		monitorExecutor.scheduleAtFixedRate(new BenchmarkReporter(benchmarkThreads), reportPeriod, reportPeriod,
				TimeUnit.SECONDS);
		BenchmarkReporter.reportHeader();
		
		///// 等待benchmark线程池中的线程都退出后, 打印测试汇总。
		if (!benchmarkExecutor.awaitTermination(runTime + 10, TimeUnit.SECONDS)) {
			LOGGER.trace("Benchmark threads still active [ count="+benchmarkExecutor.getActiveCount()+" ] when runtime is arrived, force shutdown benchmark threads pool.");
			benchmarkExecutor.shutdownNow();
		}
		Duration duration = Duration.between(benchCountingBeginTime, benchCountingEndTime);
		long realRuntime = duration.toMillis();
		
		BenchmarkReporter.reportFooter(benchmarkThreads, realRuntime);
	}
	
	class BenchmarkThread implements Callable<Void> {
		private final ReentrantLock counterLock = new ReentrantLock();
		private volatile long noTotalRuntime = 0L;
		private volatile long noMaxRuntime   = 0L;
		private volatile long noMaxRuntime2  = 0L;
		private volatile long noSucceedCount = 0L;
		private volatile long noFailureCount = 0L;
		private volatile long noRetryCount   = 0L;
		
		private volatile long pyTotalRuntime = 0L;
		private volatile long pyMaxRuntime   = 0L;
		private volatile long pyMaxRuntime2  = 0L;
		private volatile long pySucceedCount = 0L;
		private volatile long pyFailureCount = 0L;
		private volatile long pyRetryCount   = 0L;
		
		private volatile long osTotalRuntime = 0L;
		private volatile long osMaxRuntime   = 0L;
		private volatile long osMaxRuntime2  = 0L;
		private volatile long osSucceedCount = 0L;
		private volatile long osFailureCount = 0L;
		private volatile long osRetryCount   = 0L;
		
		private volatile long dlTotalRuntime = 0L;
		private volatile long dlMaxRuntime   = 0L;
		private volatile long dlMaxRuntime2  = 0L;
		private volatile long dlSucceedCount = 0L;
		private volatile long dlFailureCount = 0L;
		private volatile long dlRetryCount   = 0L;
		
		private volatile long slTotalRuntime = 0L;
		private volatile long slMaxRuntime   = 0L;
		private volatile long slMaxRuntime2  = 0L;
		private volatile long slSucceedCount = 0L;
		private volatile long slFailureCount = 0L;
		private volatile long slRetryCount   = 0L;
		
		
		@Override
		public Void call() throws Exception {
			LOGGER.trace("TPC-C benchmark thread - " + Thread.currentThread().getName() + " started.");
			try {
				while (transactionOn.get()) {
					TransactionStatistics stats = null;
					TransactionType tran = RandomHelper.randomTransaction();
					switch (tran) {
					case NewOrder:
						stats = doNewOrder();
						break;
					case Payment:
						stats = doPayment();
						break;
					case OrderStatus:
						stats = doOrdstat();
						break;
					case Delivery:
						stats = doDelivery();
						break;
					case StockLevel:
						stats = doSlev(wareCount);
						break;
					}
					//// Counting
					if (Objects.nonNull(stats) && countingOn.get()) {
						//LOGGER.info(Thread.currentThread().getName() + ": " + stats.toString());
						counterLock.lock();
						try {
							count(stats);
						} finally {
							counterLock.unlock();
						}
					}
				}
			} catch (Exception e) {
				LOGGER.error("TPC-C benchmark thread - " + Thread.currentThread().getName() + " aborted. ", e);
			}
			return null;
		}
		
		public HashMap<TransactionType, ExecutionStatistics> getBenchmarkStatistics() {
			HashMap<TransactionType, ExecutionStatistics> statistics = new HashMap<>();
			counterLock.lock();
			try {
				statistics.put(TransactionType.NewOrder, new ExecutionStatistics(noTotalRuntime, noMaxRuntime,
						noMaxRuntime2, noSucceedCount, noFailureCount, noRetryCount));
				statistics.put(TransactionType.Payment, new ExecutionStatistics(pyTotalRuntime, pyMaxRuntime,
						pyMaxRuntime2, pySucceedCount, pyFailureCount, pyRetryCount));
				statistics.put(TransactionType.OrderStatus, new ExecutionStatistics(osTotalRuntime, osMaxRuntime,
						osMaxRuntime2, osSucceedCount, osFailureCount, osRetryCount));
				statistics.put(TransactionType.Delivery, new ExecutionStatistics(dlTotalRuntime, dlMaxRuntime,
						dlMaxRuntime2, dlSucceedCount, dlFailureCount, dlRetryCount));
				statistics.put(TransactionType.StockLevel, new ExecutionStatistics(slTotalRuntime, slMaxRuntime,
						slMaxRuntime2, slSucceedCount, slFailureCount, slRetryCount));
				//// Reset runtime2
				noMaxRuntime2 = 0L;
				pyMaxRuntime2 = 0L;
				osMaxRuntime2 = 0L;
				dlMaxRuntime2 = 0L;
				slMaxRuntime2 = 0L;
			} finally {
				counterLock.unlock();
			}
			return statistics;
		}
		
		private void count(TransactionStatistics stats) {
			long runtime = stats.getElapsedTime();
			long retryCount = stats.getRetryCount();
			boolean isSuccess = stats.isSuccess();
			switch (stats.getTransactionType()) {
			case NewOrder:
				noTotalRuntime += runtime;
				noMaxRuntime = Math.max(noMaxRuntime, runtime);
				noMaxRuntime2 = Math.max(noMaxRuntime2, runtime);
				noRetryCount += retryCount;
				if (isSuccess) {
					noSucceedCount++;
				} else {
					noFailureCount++;
				}
				break;
			case Payment:
				pyTotalRuntime += runtime;
				pyMaxRuntime = Math.max(pyMaxRuntime, runtime);
				pyMaxRuntime2 = Math.max(pyMaxRuntime2, runtime);
				pyRetryCount += retryCount;
				if (isSuccess) {
					pySucceedCount++;
				} else {
					pyFailureCount++;
				}
				break;
			case OrderStatus:
				osTotalRuntime += runtime;
				osMaxRuntime = Math.max(osMaxRuntime, runtime);
				osMaxRuntime2 = Math.max(osMaxRuntime2, runtime);
				osRetryCount += retryCount;
				if (isSuccess) {
					osSucceedCount++;
				} else {
					osFailureCount++;
				}
				break;
			case Delivery:
				dlTotalRuntime += runtime;
				dlMaxRuntime = Math.max(dlMaxRuntime, runtime);
				dlMaxRuntime2 = Math.max(dlMaxRuntime2, runtime);
				dlRetryCount += retryCount;
				if (isSuccess) {
					dlSucceedCount++;
				} else {
					dlFailureCount++;
				}
				break;
			case StockLevel:
				slTotalRuntime += runtime;
				slMaxRuntime = Math.max(slMaxRuntime, runtime);
				slMaxRuntime2 = Math.max(slMaxRuntime2, runtime);
				slRetryCount += retryCount;
				if (isSuccess) {
					slSucceedCount++;
				} else {
					slFailureCount++;
				}
				break;
			default:
				break;
			}
		}
	}
	
	/**
	 * 2.4	The New-Order Transaction -> 2.4.1 Input Data Generation
	 */
	public TransactionStatistics doNewOrder() throws Exception {
		int w_id = RandomHelper.randomInt(1, wareCount);
		int d_id = RandomHelper.randomInt(1, DIST_PER_WARE);
		int c_id = RandomHelper.nuRand(1023, 1, CUST_PER_DIST);
		int ol_cnt = RandomHelper.randomInt(5, 15);
		
		int rbk = RandomHelper.randomInt(1, 100);
		int notfound = MAX_ITEMS + 1;
		int[] itemid = new int[MAX_NUM_ITEMS];
		int[] supware = new int[MAX_NUM_ITEMS];
		int[] qty = new int[MAX_NUM_ITEMS];
		int o_all_local = 1;
		for (int i = 0; i < ol_cnt; i++) {
			itemid[i] = RandomHelper.nuRand(8191, 1, MAX_ITEMS);
			if ((i == (ol_cnt - 1)) && (rbk == 1)) {
				itemid[i] = notfound;
			}
			if (RandomHelper.randomInt(1, 100) != 1) {
				supware[i] = w_id;
			} else {
				supware[i] = TpccTransaction.otherWare(w_id, wareCount);
				o_all_local = 0;
			}
			qty[i] = RandomHelper.randomInt(1, 10);
		}
		
		boolean success = false;
		long runTime = 0L;
		int retry = 0;
		LocalDateTime beginTime = LocalDateTime.now();
		for (; retry < MAX_RETRY; retry++) {
			try (Connection connection = dataSource.getConnection()) {
				success = TpccTransaction.newOrder(connection, dbms, w_id, d_id, c_id, ol_cnt, o_all_local, itemid, supware, qty);
				if (success) {
					break;
				}
			} catch (NoDataFoundException e) {
				LOGGER.trace("New-order transaction failed with 'NO_DATA_FOUND'", e);
				// 不重试, 因为传参不变, 结果肯定还是NO_DATA_FOUND, retry没有意义.
				break;
			} catch (Exception e) {
				LOGGER.trace("New-order transaction failed. Retries (" + retry + ")", e);
			}
		}
		LocalDateTime endTime = LocalDateTime.now();
		Duration duration = Duration.between(beginTime, endTime);
		runTime = duration.toMillis();
		TransactionStatistics transStat = new TransactionStatistics(TransactionType.NewOrder, success, runTime, retry);
		LOGGER.trace("transaction done: " + transStat.toString());
		return transStat;
	}
	
	
	/**
	 * 2.5 The Payment Transaction -> 2.5.1 Input Data Generation
	 */
	public TransactionStatistics doPayment() throws Exception {
		int w_id = RandomHelper.randomInt(1, wareCount);
		int d_id = RandomHelper.randomInt(1, DIST_PER_WARE);
		int c_id = RandomHelper.nuRand(1023, 1, CUST_PER_DIST);
		String c_last = RandomHelper.lastName(RandomHelper.nuRand(255, 0, 999));
		int h_amount = RandomHelper.randomInt(1, 5000);
		boolean byname;
		if (RandomHelper.randomInt(1, 100) <= 60) {
			byname = true; /* select by last name */
		} else {
			byname = false; /* select by customer id */
		}
		int c_w_id, c_d_id;
		if (RandomHelper.randomInt(1, 100) <= 85) {
			c_w_id = w_id;
			c_d_id = d_id;
		} else {
			c_w_id = TpccTransaction.otherWare(w_id, wareCount);
			c_d_id = RandomHelper.randomInt(1, DIST_PER_WARE);
		}
		
		boolean success = false;
		long runTime = 0L;
		int retry = 0;
		LocalDateTime beginTime = LocalDateTime.now();
		for (; retry < MAX_RETRY; retry++) {
			try (Connection connection = dataSource.getConnection()) {
				success = TpccTransaction.payment(connection, dbms, w_id, d_id, byname, c_w_id, c_d_id, c_id, c_last, h_amount);
				if (success) {
					break;
				}
			} catch (NoDataFoundException e) {
				LOGGER.trace("Payment transaction failed with 'NO_DATA_FOUND'", e);
				break;
			} catch (Exception e) {
				LOGGER.trace("Payment transaction failed. Retries (" + retry + ")", e);
			}
		}
		LocalDateTime endTime = LocalDateTime.now();
		Duration duration = Duration.between(beginTime, endTime);
		runTime = duration.toMillis();
		TransactionStatistics transStat = new TransactionStatistics(TransactionType.Payment, success, runTime, retry);
		LOGGER.trace("transaction done: " + transStat.toString());
		return transStat;
	}

	
	/**
	 * 2.6 The Order-Status Transaction -> 2.6.1 Input Data Generation
	 */
	public TransactionStatistics doOrdstat() throws Exception {
		int w_id = RandomHelper.randomInt(1, wareCount);
		int d_id = RandomHelper.randomInt(1, DIST_PER_WARE);
		int c_id = RandomHelper.nuRand(1023, 1, CUST_PER_DIST);
		String c_last = RandomHelper.lastName(RandomHelper.nuRand(255, 0, 999));
		boolean byname;
		if (RandomHelper.randomInt(1, 100) <= 60) {
			byname = true; /* select by last name */
		} else {
			byname = false; /* select by customer id */
		}
		boolean success = false;
		long runTime = 0L;
		int retry = 0;
		LocalDateTime beginTime = LocalDateTime.now();
		for (; retry < MAX_RETRY; retry++) {
			try (Connection connection = dataSource.getConnection()) {
				success = TpccTransaction.ordstat(connection, dbms, w_id, d_id, byname, c_id, c_last);
				if (success) {
					break;
				}
			} catch (NoDataFoundException e) {
				LOGGER.trace("Order-Status transaction failed with 'NO_DATA_FOUND'", e);
				break;
			} catch (Exception e) {
				LOGGER.trace("Order-Status transaction failed. Retries (" + retry + ")", e);
			}
		}
		LocalDateTime endTime = LocalDateTime.now();
		Duration duration = Duration.between(beginTime, endTime);
		runTime = duration.toMillis();
		TransactionStatistics transStat = new TransactionStatistics(TransactionType.OrderStatus, success, runTime, retry);
		LOGGER.trace("transaction done: " + transStat.toString());
		return transStat;
	}


	/**
	 * 2.7 The Delivery Transaction -> 2.7.1 Input Data Generation
	 */
	public TransactionStatistics doDelivery() throws Exception {
		int w_id = RandomHelper.randomInt(1, wareCount);
		int o_carrier_id = RandomHelper.randomInt(1, 10);
		boolean success = false;
		long runTime = 0L;
		int retry = 0;
		LocalDateTime beginTime = LocalDateTime.now();
		for (; retry < MAX_RETRY; retry++) {
			try (Connection connection = dataSource.getConnection()) {
				success = TpccTransaction.delivery(connection, dbms, w_id, o_carrier_id);
				if (success) {
					break;
				}
			} catch (NoDataFoundException e) {
				LOGGER.trace("Delivery transaction failed with 'NO_DATA_FOUND'", e);
				break;
			} catch (Exception e) {
				LOGGER.trace("Delivery transaction failed. Retries (" + retry + ")", e);
			}
		}
		LocalDateTime endTime = LocalDateTime.now();
		Duration duration = Duration.between(beginTime, endTime);
		runTime = duration.toMillis();
		TransactionStatistics transStat = new TransactionStatistics(TransactionType.Delivery, success, runTime, retry);
		LOGGER.trace("transaction done: " + transStat.toString());
		return transStat;
	}
	
	/**
	 * 2.8 The Stock-Level Transaction -> 2.8.1 Input Data Generation   
	 */
	public TransactionStatistics doSlev(int num_ware) throws Exception {
		int w_id, d_id, level;
		w_id = RandomHelper.randomInt(1, num_ware);
		d_id = RandomHelper.randomInt(1, DIST_PER_WARE);
		level = RandomHelper.randomInt(10, 20);
		boolean success = false;
		long runTime = 0L;
		int retry = 0;
		LocalDateTime beginTime = LocalDateTime.now();
		for (; retry < MAX_RETRY; retry++) {
			try (Connection connection = dataSource.getConnection()) {
				success = TpccTransaction.slev(connection, dbms, w_id, d_id, level);
				if (success) {
					break;
				}
			} catch (NoDataFoundException e) {
				LOGGER.trace("Stock-Level transaction failed with 'NO_DATA_FOUND'", e);
				break;
			} catch (Exception e) {
				LOGGER.trace("Stock-Level transaction failed. Retries (" + retry + ")", e);
			}
		}
		LocalDateTime endTime = LocalDateTime.now();
		Duration duration = Duration.between(beginTime, endTime);
		runTime = duration.toMillis();
		TransactionStatistics transStat = new TransactionStatistics(TransactionType.StockLevel, success, runTime, retry);
		LOGGER.trace("transaction done: " + transStat.toString());
		return transStat;
	}
}
