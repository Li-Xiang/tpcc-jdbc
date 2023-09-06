package org.littlestar.tpcc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.littlestar.tpcc.TpccDriver.BenchmarkThread;
import org.littlestar.tpcc.datasource.TpccDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkReporter implements Runnable {
	private final static Logger LOGGER = LoggerFactory.getLogger(BenchmarkReporter.class);
	private final List<BenchmarkThread> benchmarkThreads;
	private long lastNoTotalRuntime = 0L;
	private long lastNoSucceedCount = 0L;
	private long lastPyTotalRuntime = 0L;
	private long lastPySucceedCount = 0L;
	private long lastOsTotalRuntime = 0L;
	private long lastOsSucceedCount = 0L;
	private long lastDlTotalRuntime = 0L;
	private long lastDlSucceedCount = 0L;
	private long lastSlTotalRuntime = 0L;
	private long lastSlSucceedCount = 0L;
	
	LocalDateTime beginTime;
	
	public BenchmarkReporter(List<BenchmarkThread> benchmarkThreads) {
		this.benchmarkThreads = benchmarkThreads;
		this.beginTime = LocalDateTime.now();
	}
	
	@Override
	public void run() {
		ArrayList<HashMap<TransactionType, ExecutionStatistics>> currAllStats = new ArrayList<>();
		for (BenchmarkThread benchmarkThread : benchmarkThreads) {
			currAllStats.add(benchmarkThread.getBenchmarkStatistics());
		}
		LocalDateTime endTime = LocalDateTime.now();
		Duration duration = Duration.between(beginTime, endTime);
		beginTime = endTime;
		long interval = duration.toMillis();
		
		long currNoTotalRuntime = 0L;
		long currNoMaxRuntime   = 0L;
		long currNoSucceedCount = 0L;
		
		long currPyTotalRuntime = 0L;
		long currPyMaxRuntime   = 0L;
		long currPySucceedCount = 0L;
		
		long currOsTotalRuntime = 0L;
		long currOsMaxRuntime   = 0L;
		long currOsSucceedCount = 0L;
		
		long currDlTotalRuntime = 0L;
		long currDlMaxRuntime   = 0L;
		long currDlSucceedCount = 0L;
		
		long currSlTotalRuntime = 0L;
		long currSlMaxRuntime   = 0L;
		long currSlSucceedCount = 0L;
		
		for (HashMap<TransactionType, ExecutionStatistics> stats : currAllStats) {
			if (stats.containsKey(TransactionType.NewOrder)) {
				ExecutionStatistics no = stats.get(TransactionType.NewOrder);
				currNoTotalRuntime += no.getRuntime();
				currNoSucceedCount += no.getSucceedCount();
				currNoMaxRuntime = Math.max(currNoMaxRuntime, no.getMaxRuntime2());
			} else {
				currNoTotalRuntime = lastNoTotalRuntime;
				currNoSucceedCount = lastNoSucceedCount;
			}

			if (stats.containsKey(TransactionType.Payment)) {
				ExecutionStatistics py = stats.get(TransactionType.Payment);
				currPyTotalRuntime += py.getRuntime();
				currPySucceedCount += py.getSucceedCount();
				currPyMaxRuntime = Math.max(currPyMaxRuntime, py.getMaxRuntime2());
			} else {
				currPyTotalRuntime = lastPyTotalRuntime;
				currPySucceedCount = lastPySucceedCount;
			}

			if (stats.containsKey(TransactionType.OrderStatus)) {
				ExecutionStatistics os = stats.get(TransactionType.OrderStatus);
				currOsTotalRuntime += os.getRuntime();
				currOsSucceedCount += os.getSucceedCount();
				currOsMaxRuntime = Math.max(currOsMaxRuntime, os.getMaxRuntime2());
			} else {
				currOsTotalRuntime = lastOsTotalRuntime;
				currOsSucceedCount = lastOsSucceedCount;
			}
			
			if (stats.containsKey(TransactionType.Delivery)) {
				ExecutionStatistics dl = stats.get(TransactionType.Delivery);
				currDlTotalRuntime += dl.getRuntime();
				currDlSucceedCount += dl.getSucceedCount();
				currDlMaxRuntime = Math.max(currDlMaxRuntime, dl.getMaxRuntime2());
			} else {
				currDlTotalRuntime = lastDlTotalRuntime;
				currDlSucceedCount = lastDlSucceedCount;
			}
			
			if (stats.containsKey(TransactionType.StockLevel)) {
				ExecutionStatistics sl = stats.get(TransactionType.StockLevel);
				currSlTotalRuntime += sl.getRuntime();
				currSlSucceedCount += sl.getSucceedCount();
				currSlMaxRuntime = Math.max(currSlMaxRuntime, sl.getMaxRuntime2());
			} else {
				currSlTotalRuntime = lastSlTotalRuntime;
				currSlSucceedCount = lastSlSucceedCount;
			}
		}
		long deltaNoTotalRuntime = currNoTotalRuntime - lastNoTotalRuntime;
		long deltaNoSucceedCount = currNoSucceedCount - lastNoSucceedCount;
		long deltaPyTotalRuntime = currPyTotalRuntime - lastPyTotalRuntime;
		long deltaPySucceedCount = currPySucceedCount - lastPySucceedCount;
		long deltaOsTotalRuntime = currOsTotalRuntime - lastOsTotalRuntime;
		long deltaOsSucceedCount = currOsSucceedCount - lastOsSucceedCount;
		long deltaDlTotalRuntime = currDlTotalRuntime - lastDlTotalRuntime;
		long deltaDlSucceedCount = currDlSucceedCount - lastDlSucceedCount;
		long deltaSlTotalRuntime = currSlTotalRuntime - lastSlTotalRuntime;
		long deltaSlSucceedCount = currSlSucceedCount - lastSlSucceedCount;
		long deltaSucceedCount = deltaNoSucceedCount + deltaPySucceedCount + deltaOsSucceedCount + deltaDlSucceedCount
				+ deltaSlSucceedCount;
		lastNoTotalRuntime = currNoTotalRuntime;
		lastNoSucceedCount = currNoSucceedCount;
		lastPyTotalRuntime = currPyTotalRuntime;
		lastPySucceedCount = currPySucceedCount;
		lastOsTotalRuntime = currOsTotalRuntime;
		lastOsSucceedCount = currOsSucceedCount;
		lastDlTotalRuntime = currDlTotalRuntime;
		lastDlSucceedCount = currDlSucceedCount;
		lastSlTotalRuntime = currSlTotalRuntime;
		lastSlSucceedCount = currSlSucceedCount;
		
		long noAvgRt = (deltaNoSucceedCount == 0L) ? 0L : (deltaNoTotalRuntime / deltaNoSucceedCount);
		long pyAvgRt = (deltaPySucceedCount == 0L) ? 0L : (deltaPyTotalRuntime / deltaPySucceedCount);
		long osAvgRt = (deltaOsSucceedCount == 0L) ? 0L : (deltaOsTotalRuntime / deltaOsSucceedCount);
		long dlAvgRt = (deltaDlSucceedCount == 0L) ? 0L : (deltaDlTotalRuntime / deltaDlSucceedCount);
		long slAvgRt = (deltaSlSucceedCount == 0L) ? 0L : (deltaSlTotalRuntime / deltaSlSucceedCount);
		
		
		long totalTps = Math.round((deltaSucceedCount * 1000.0D) / interval);
		long noTps = Math.round((deltaNoSucceedCount * 1000.0D) / interval);
		long pyTps = Math.round((deltaPySucceedCount * 1000.0D) / interval);
		long osTps = Math.round((deltaOsSucceedCount * 1000.0D) / interval);
		long dlTps = Math.round((deltaDlSucceedCount * 1000.0D) / interval);
		long slTps = Math.round((deltaSlSucceedCount * 1000.0D) / interval);
		
		reportRow(totalTps, 
				noTps, noAvgRt, currNoMaxRuntime,
				pyTps, pyAvgRt, currPyMaxRuntime,
				osTps, osAvgRt, currOsMaxRuntime,
				dlTps, dlAvgRt, currDlMaxRuntime,
				slTps, slAvgRt, currSlMaxRuntime
				);
		
	}
	
	public static void reportRow(long totalTps, 
			long noTps, long noAvgRt, long noMaxRt,
			long pyTps, long pyAvgRt, long pyMaxRt,
			long osTps, long osAvgRt, long osMaxRt,
			long dlTps, long dlAvgRt, long dlMaxRt,
			long slTps, long slAvgRt, long slMaxRt
			) {
		String totalColumn = String.format("| %6s ", totalTps);
		String newOrderColumn = String.format("| %5s %5s %5s ", noTps, noAvgRt, noMaxRt);
		String playmentColumn = String.format("| %5s %5s %5s ", pyTps, pyAvgRt, pyMaxRt);
		String orderStatusColumn = String.format("| %5s %5s %5s ", osTps, osAvgRt, osMaxRt);
		String deliveryColumn = String.format("| %5s %5s %5s ", dlTps, dlAvgRt, dlMaxRt);
		String stockLevelColumn = String.format("| %5s %5s %5s |", slTps, slAvgRt, slMaxRt);
		output2(totalColumn + newOrderColumn + playmentColumn + orderStatusColumn + deliveryColumn + stockLevelColumn);
	}
	
	public static void reportWelcome(TpccDataSource ds, int warehouses, int threads) {
		String dataSourceName = ds.getDataSourceClassName();
		String dbmsInfo ="n/a";
		String driverInfo = "n/a";
		String url = "n/a";
		try (Connection connection = ds.getConnection()) {
			DatabaseMetaData metaData = connection.getMetaData();
			url = metaData.getURL();
			driverInfo = metaData.getDriverName() +" / " + metaData.getDriverVersion();
			dbmsInfo = metaData.getDatabaseProductName() +" / " + metaData.getDatabaseProductVersion();
		} catch (Exception e) {
			LOGGER.trace("get dbms info failed.", e);
		}
		
		StringBuilder welcome = new StringBuilder();
		welcome.append("******************************************************************\n")
		       .append("TPC-C Load Generator (for JDBC)\n")
			   .append("  [DataSource]: ").append(dataSourceName).append("\n")
			   .append("  [Driver]    : ").append(driverInfo).append("\n")
			   .append("  [URL]       : ").append(url).append("\n")
			   .append("  [DBMS]      : ").append(dbmsInfo).append("\n")
			   .append("  [Warehouse] : ").append(warehouses).append("\n")
			   .append("  [Threads]   : ").append(threads).append("\n")
		       .append("******************************************************************\n");
		output(welcome.toString());
	}
	
	private static void output2(String msg) {
		System.out.println(getTimestampString() + " " + msg);
	}

	private static void output(String msg) {
		System.out.println(msg);
	}
	
	public static void reportHeader() {
		output("         | Total  |    New-Order      |     Payment       |   Order-Status    |     Delivery      |    Stock-Level    |");
		output("         |  TPs/  | TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/|");
		output("---------+--------+-------------------+-------------------+-------------------+-------------------+-------------------+");
	}
	
	public static void reportFooter(List<BenchmarkThread> benchmarkThreads, long benchCountingMs) {
		
		long noTxs  = 0L, pyTxs  = 0L, osTxs  = 0L, dlTxs  = 0L, slTxs  = 0L;
		long noRt   = 0L, pyRt   = 0L, osRt   = 0L, dlRt   = 0L, slRt   = 0L;
		long noMxRt = 0L, pyMxRt = 0L, osMxRt = 0L, dlMxRt = 0L, slMxRt = 0L;
		long noFl   = 0L, pyFl   = 0L, osFl   = 0L, dlFl   = 0L, slFl   = 0L;
		long noRy   = 0L, pyRy   = 0L, osRy   = 0L, dlRy   = 0L, slRy   = 0L;
		
		for (BenchmarkThread benchmarkThread : benchmarkThreads) {
			HashMap<TransactionType, ExecutionStatistics> statsMap = benchmarkThread.getBenchmarkStatistics();
			///// New-Order
			if (statsMap.containsKey(TransactionType.NewOrder)) {
				ExecutionStatistics noStats = statsMap.get(TransactionType.NewOrder);
				noTxs += noStats.getSucceedCount();
				noRt += noStats.getRuntime();
				noMxRt = Math.max(noMxRt, noStats.getMaxRuntime());
				noFl += noStats.getFailureCount();
				noRy += noStats.getRetryCount();
			}
			///// Payment
			if (statsMap.containsKey(TransactionType.Payment)) {
				ExecutionStatistics pyStats = statsMap.get(TransactionType.Payment);
				pyTxs += pyStats.getSucceedCount();
				pyRt += pyStats.getRuntime();
				pyMxRt = Math.max(pyMxRt, pyStats.getMaxRuntime());
				pyFl += pyStats.getFailureCount();
				pyRy += pyStats.getRetryCount();
			}
			///// Order-Status
			if (statsMap.containsKey(TransactionType.OrderStatus)) {
				ExecutionStatistics osStats = statsMap.get(TransactionType.OrderStatus);
				osTxs += osStats.getSucceedCount();
				osRt += osStats.getRuntime();
				osMxRt = Math.max(osMxRt, osStats.getMaxRuntime());
				osFl += osStats.getFailureCount();
				osRy += osStats.getRetryCount();
			}
			///// Delivery
			if (statsMap.containsKey(TransactionType.Delivery)) {
				ExecutionStatistics dlStats = statsMap.get(TransactionType.Delivery);
				dlTxs += dlStats.getSucceedCount();
				dlRt += dlStats.getRuntime();
				dlMxRt = Math.max(dlMxRt, dlStats.getMaxRuntime());
				dlFl += dlStats.getFailureCount();
				dlRy += dlStats.getRetryCount();
			}
			///// Stock-Level
			if (statsMap.containsKey(TransactionType.StockLevel)) {
				ExecutionStatistics slStats = statsMap.get(TransactionType.StockLevel);
				slTxs += slStats.getSucceedCount();
				slRt += slStats.getRuntime();
				slMxRt = Math.max(slMxRt, slStats.getMaxRuntime());
				slFl += slStats.getFailureCount();
				slRy += slStats.getRetryCount();
			}
		}

		double totalTxs = noTxs + pyTxs + osTxs + dlTxs + slTxs;
		double totalTps = (benchCountingMs > 0) ? (totalTxs * 1000.0D / benchCountingMs) : 0.0D;
		double totalTpmc = totalTps * 60.0D;
		//// New-Order
		double noTps = (benchCountingMs > 0) ? (noTxs * 1000.0D / benchCountingMs) : 0.0D;
		double noTpmc = noTps * 60.0D;
		double noAvgRt = (noTxs > 0) ? ((double) noRt / noTxs) : 0.0D;
		double noTxPct = (totalTxs > 0) ? ((double) noTxs / totalTxs) * 100.0D : 0.0D;

		//// Payment
		double pyTps = (benchCountingMs > 0) ? (pyTxs * 1000.0D / benchCountingMs) : 0.0D;
		double pyTpmc = pyTps * 60.0D;
		double pyAvgRt = (pyTxs > 0) ? ((double) pyRt / pyTxs) : 0.0D;
		double pyTxPct = (totalTxs > 0) ? ((double) pyTxs / totalTxs) * 100.0D : 0.0D;

		//// Order-Status
		double osTps = (benchCountingMs > 0) ? (1000.0D * osTxs / benchCountingMs) : 0.0D;
		double osTpmc = osTps * 60.0D;
		double osAvgRt = (osTxs > 0) ? ((double) osRt / osTxs) : 0.0D;
		double osTxPct = (totalTxs > 0) ? ((double) osTxs / totalTxs) * 100.0D : 0.0D;

		//// Delivery
		double dlTps = (benchCountingMs > 0) ? (1000.0D * dlTxs / benchCountingMs) : 0.0D;
		double dlTpmc = dlTps * 60.0D;
		double dlAvgRt = (dlTxs > 0) ? ((double) dlRt / dlTxs) : 0.0D;
		double dlTxPct = (totalTxs > 0) ? ((double) dlTxs / totalTxs) * 100.0D : 0.0D;

		//// Stock-Level
		double slTps = (benchCountingMs > 0) ? (1000.0D * slTxs / benchCountingMs) : 0.0D;
		double slTpmc = slTps * 60.0D;
		double slAvgRt = (slTxs > 0) ? ((double) slRt / slTxs) : 0.0D;
		double slTxPct = (totalTxs > 0) ? ((double) slTxs / totalTxs) * 100.0D : 0.0D;
		
		output(String.format("\nTPC-C Benchmark Completed: Runtime %s ms,  %.2f TpmC, %.2f Tps.", benchCountingMs, totalTpmc, totalTps));
		output(String.format("     New-Order -> TX: %s (Failed: %s, Retries: %s), Tpmc: %.2f, Tps: %.2f, Avg-Rt: %.2f ms, Max-Rt: %s ms, ofTotal: %.2f %%"                , noTxs, noFl, noRy, noTpmc, noTps, noAvgRt, noMxRt, noTxPct));
		output(String.format("       Payment -> TX: %s (Failed: %s, Retries: %s), Tpmc: %.2f, Tps: %.2f, Avg-Rt: %.2f ms, Max-Rt: %s ms, ofTotal: %.2f %% (>43.0%% is OK)", pyTxs, pyFl, pyRy, pyTpmc, pyTps, pyAvgRt, pyMxRt, pyTxPct));
		output(String.format("  Order-Status -> TX: %s (Failed: %s, Retries: %s), Tpmc: %.2f, Tps: %.2f, Avg-Rt: %.2f ms, Max-Rt: %s ms, ofTotal: %.2f %% (> 4.0%% is OK)", osTxs, osFl, osRy, osTpmc, osTps, osAvgRt, osMxRt, osTxPct));
		output(String.format("      Delivery -> TX: %s (Failed: %s, Retries: %s), Tpmc: %.2f, Tps: %.2f, Avg-Rt: %.2f ms, Max-Rt: %s ms, ofTotal: %.2f %% (> 4.0%% is OK)", dlTxs, dlFl, dlRy, dlTpmc, dlTps, dlAvgRt, dlMxRt, dlTxPct));
		output(String.format("   Stock-Level -> TX: %s (Failed: %s, Retries: %s), Tpmc: %.2f, Tps: %.2f, Avg-Rt: %.2f ms, Max-Rt: %s ms, ofTotal: %.2f %% (> 4.0%% is OK)\n", slTxs, slFl, slRy, slTpmc, slTps, slAvgRt, slMxRt, slTxPct));
	}
	
	public static String getTimestampString() {
		return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
	}
	
}