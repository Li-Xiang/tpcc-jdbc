package org.littlestar.tpcc;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

import org.littlestar.tpcc.datasource.HikariCPBuilder;
import org.littlestar.tpcc.datasource.NonPoolDataSource;
import org.littlestar.tpcc.datasource.TpccDataSource;
import org.apache.logging.log4j.Level;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class BenchmarkBuilder {
	private BenchmarkConfig benchmarkConfig;
	public BenchmarkBuilder() {
		benchmarkConfig = new BenchmarkConfig();
	}
	
	public BenchmarkBuilder(BenchmarkConfig benchmarkConfig) {
		this.benchmarkConfig = benchmarkConfig;
	}
	
	public BenchmarkBuilder(String configFile) throws Exception {
		try (BufferedReader reader = Files.newBufferedReader(Paths.get(configFile))) {
			Gson gson = new Gson();
			benchmarkConfig = gson.fromJson(reader, BenchmarkConfig.class);
		}
	}
	
	public BenchmarkBuilder withBenchmarkProfile(Properties properties) {
		benchmarkConfig.setBenchmarkProfile(properties);
		return this;
	}
	
	public BenchmarkBuilder withConnectionPoolProfile(Properties properties) {
		benchmarkConfig.setConnectionPoolProfile(properties);
		return this;
	}
	
	public BenchmarkBuilder withDataSourceProfile(Properties properties) {
		benchmarkConfig.setDataSourceProfile(properties);
		return this;
	}
	
	public void execute(String command) throws Exception {
		Dbms dbms = benchmarkConfig.getDbms();
		int wareCount = benchmarkConfig.getWarehouses();
		int threads = benchmarkConfig.getThreads();
		Level logLevel = benchmarkConfig.getLogLevel();
		Log4jInitializer.initialize(logLevel).withConsoleAppender(Level.ALL).withLevel("com.zaxxer.hikari",
				Level.ERROR);
		Properties cpProfile = benchmarkConfig.getConnectionPoolProfile();
		Properties dsProfile = benchmarkConfig.getDataSourceProfile();
		TpccDataSource ds = null;
		try {
			if (cpProfile.isEmpty()) {
				if (dsProfile.containsKey("url") && dsProfile.containsKey("url")) {
					String url = dsProfile.getProperty("url");
					String driver = dsProfile.getProperty("driver");
					ds = new NonPoolDataSource(url, driver);
				} else {
					throw new Exception("Missing url and driver parameters");
				}
			} else {
				ds = new HikariCPBuilder().withHikariProperties(cpProfile).withDataSourceProperties(dsProfile)
						.createDataSource();
			}

			if (Objects.isNull(command)) {
				throw new Exception("Command is null");
			} else if (Objects.equals(command.toLowerCase(), Benchmark.LOAD_COMMAND)) {
				TpccLoader loader = new TpccLoader(ds, dbms, wareCount, threads);
				loader.createTpccTables();
				loader.doLoad();
				loader.doAddFksAndIndexes(true);
				loader.doGatherStatistics();
				loader.checkTableRows();
			} else if (Objects.equals(command.toLowerCase(), Benchmark.ADDFK_COMMAND)) {
				new TpccLoader(ds, dbms, wareCount, threads).doAddForeignKeys();
			} else if (Objects.equals(command.toLowerCase(), Benchmark.DROPFK_COMMAND)) {
				new TpccLoader(ds, dbms, wareCount, threads).doDropForeignKeys();
			} else if (Objects.equals(command.toLowerCase(), Benchmark.GATHERSTATS_COMMAND)) {
				new TpccLoader(ds, dbms, wareCount, threads).doGatherStatistics();
			} else if (Objects.equals(command.toLowerCase(), Benchmark.CHECK_COMMAND)) {
				new TpccLoader(ds, dbms, wareCount, threads).checkTableRows();
			} else if (Objects.equals(command.toLowerCase(), Benchmark.BENCHMARK_COMMAND)) {
				int runTime = benchmarkConfig.getRunTime();
				int rampUp = benchmarkConfig.getRampUp();
				int reportInterval = benchmarkConfig.getReportInterval();
				TpccDriver tpccDriver = new TpccDriver(ds, dbms, wareCount);
				tpccDriver.benchmark(runTime, rampUp, reportInterval, threads);
			} else if (Objects.equals(command.toLowerCase(), Benchmark.DROP_COMMAND)) {
				new TpccLoader(ds, dbms, wareCount, threads).doDropTables();
			} else {
				throw new Exception("Unsupport command '" + command + "'");
			}
		} catch (Exception e) {
			throw e;
		} finally {
			ds.close();
		}
	}
	
	@Override
	public String toString() {
		Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
		String json = gson.toJson(benchmarkConfig);
		return json;
	}
}
