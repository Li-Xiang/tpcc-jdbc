package org.littlestar.tpcc;

import java.util.Objects;
import java.util.Properties;
import org.apache.logging.log4j.Level;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class BenchmarkConfig {
	public static final Dbms    DBMS_DEFAULT       = Dbms.Unknown;
	public static final String  DBMS_OPTION       = "dbms";
	public static final Integer WAREHOUSES_DEFAULT = 5;
	public static final String  WAREHOUSES_OPTION  = "warehouses";
	public static final Integer THREADS_DEFALUT    = 5;
	public static final String  THREADS_OPTION     = "threads";
	public static final Integer RAMPUP_DEFAULT     = 20;
	public static final String  RAMPUP_OPTION      = "ramp-up";
	public static final Integer RUNTIME_DEFAULT    = 120;
	public static final String  RUNTIME_OPTION     = "runtime";
	public static final Level   LOG_LEVEL_DEFAULT  = Level.INFO;
	public static final String  LOG_LEVEL_OPTION   = "log-level";
	public static final Integer REPORT_INTERVAL_DEFAULT = 5;
	public static final String  REPORT_INTERVAL_OPTION  = "report-interval";
	
	private final Properties benchmark;
	private final Properties connectionPool;
	private final Properties dataSource;

	public BenchmarkConfig(Properties benchmark, Properties connectionPool, Properties dataSource) {
		this.benchmark = benchmark;
		this.connectionPool = connectionPool;
		this.dataSource = dataSource;
	}
	
	public Dbms getDbms() {
		try {
			String value = benchmark.getProperty(DBMS_OPTION);
			return Dbms.parse(value);
		} catch (Exception e) {
			return DBMS_DEFAULT;
		}
	}
	
	public int getWarehouses() {
		try {
			String value = benchmark.getProperty(WAREHOUSES_OPTION);
			int warehouses = Integer.parseInt(value);
			return warehouses > 0 ? warehouses : WAREHOUSES_DEFAULT;
		} catch (Exception e) {
			return WAREHOUSES_DEFAULT;
		}
	}
	
	public int getThreads() {
		try {
			String value = benchmark.getProperty(THREADS_OPTION);
			int threads = Integer.parseInt(value);
			return threads > 0 ? threads : THREADS_DEFALUT;
		} catch (Exception e) {
			return THREADS_DEFALUT;
		}
	}
	
	public int getRampUp() {
		try {
			String value = benchmark.getProperty(RAMPUP_OPTION);
			int rampup = Integer.parseInt(value);
			return rampup >= 0 ? rampup : RAMPUP_DEFAULT;
		} catch (Exception e) {
			return RAMPUP_DEFAULT;
		}
	}
	
	public int getRunTime() {
		try {
			String value = benchmark.getProperty(RUNTIME_OPTION);
			int runtime = Integer.parseInt(value);
			return runtime > 0 ? runtime : RUNTIME_DEFAULT;
		} catch (Exception e) {
			return RUNTIME_DEFAULT;
		}
	}
	
	public int getReportInterval() {
		try {
			String value = benchmark.getProperty(REPORT_INTERVAL_OPTION);
			int reportInterval = Integer.parseInt(value);
			return reportInterval > 0 ? reportInterval : REPORT_INTERVAL_DEFAULT;
		} catch (Exception e) {
			return REPORT_INTERVAL_DEFAULT;
		}
	}
	
	public Level getLogLevel() {
		try {
			String value = benchmark.getProperty(LOG_LEVEL_OPTION);
			Level level = Level.getLevel(value.toUpperCase());
			return level;
		} catch (Exception e) {
			return LOG_LEVEL_DEFAULT;
		}
	}
	
	public BenchmarkConfig(Properties connectionPool, Properties dataSources) {
		this(benchmarkDefaultProfile(), connectionPool, dataSources);
	}
	
	public BenchmarkConfig() {
		this(benchmarkDefaultProfile(), new Properties(), new Properties());
	}
	
	public void setBenchmarkProfile(Properties properties) {
		if (Objects.nonNull(properties)) {
			benchmark.putAll(properties);
		}
	}
	
	public Properties getBenchmarkProfile() {
		return benchmark;
	}
	
	public void setConnectionPoolProfile(Properties properties) {
		if (Objects.nonNull(properties)) {
			connectionPool.putAll(properties);
		}
	}
	
	public Properties getConnectionPoolProfile() {
		return connectionPool;
	}
	
	public void setDataSourceProfile(Properties properties) {
		if (Objects.nonNull(properties)) {
			dataSource.putAll(properties);
		}
	}
	
	public Properties getDataSourceProfile() {
		return dataSource;
	}
	
	
	public static Properties benchmarkDefaultProfile() {
		Properties properties = new Properties();
		properties.setProperty(DBMS_OPTION, Dbms.MySQL.toString());
		properties.setProperty(WAREHOUSES_OPTION, "5");
		properties.setProperty(THREADS_OPTION, "5");
		properties.setProperty(RAMPUP_OPTION, "10");
		properties.setProperty(RUNTIME_OPTION, "3600");
		properties.setProperty(LOG_LEVEL_OPTION, "info");
		return properties;
	}
	/*
	public static Properties connecitonPoolDefaultProfileForMySQL() {
		Properties properties = new Properties();
		properties.setProperty(HikariCPBuilder.JDBCURL, "jdbc:mysql://127.0.0.1/tpcc");
		properties.setProperty(HikariCPBuilder.USERNAME, "root");
		properties.setProperty(HikariCPBuilder.PASSWORD, "Passw0rd");
		properties.setProperty(HikariCPBuilder.MAXIMUMPOOLSIZE, "20");
		return properties;
	}
	
	public static Properties dataSourceDefaultProfileForMySQL() {
		Properties properties = new Properties();
		properties.setProperty("useSSL", "false");
		properties.setProperty("characterEncoding", "UTF-8");
		properties.setProperty("autoReconnect", "true");
		properties.setProperty("rewriteBatchedStatements", "true");
		//	
		properties.setProperty("cachePrepStmts", "true");
		properties.setProperty("prepStmtCacheSize", "250");
		properties.setProperty("prepStmtCacheSqlLimit", "2048");
		properties.setProperty("useServerPrepStmts","true");
		properties.setProperty("useLocalSessionState","true");
		properties.setProperty("cacheResultSetMetadata","true");
		properties.setProperty("cacheServerConfiguration","true");
		properties.setProperty("elideSetAutoCommits","true");
		properties.setProperty("maintainTimeStats","false");
		return properties;
	}
	*/
	@Override
	public String toString() {
		Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
		String json = gson.toJson(this);
		return json;
	}

}
