package org.littlestar.tpcc.datasource;

import java.util.Properties;
import com.zaxxer.hikari.HikariDataSource;

public class HikariCPBuilder {
	// HikariCP参数: https://github.com/brettwooldridge/HikariCP
	//// Essentials
	/**
	 * This is the name of the DataSource class provided by the JDBC driver. Consult the documentation for your specific 
	 * JDBC driver to get this class name, or see the table below. Note XA data sources are not supported. XA requires a 
	 * real transaction manager like bitronix. Note that you do not need this property if you are using jdbcUrl for "old-school" 
	 * DriverManager-based JDBC driver configuration. 
	 * Default: none
	 * */
	public static final String DATASOURCECLASSNAME  = "dataSourceClassName";
	
	/**
	 * This property directs HikariCP to use "DriverManager-based" configuration. We feel that DataSource-based configuration (above) 
	 * is superior for a variety of reasons (see below), but for many deployments there is little significant difference. When using 
	 * this property with "old" drivers, you may also need to set the driverClassName property, but try it first without. Note that 
	 * if this property is used, you may still use DataSource properties to configure your driver and is in fact recommended over driver 
	 * parameters specified in the URL itself. 
	 * 
	 * Default: none
	 */
	public static final String JDBCURL              = "jdbcUrl";
	
	/**
	 * This property sets the default authentication username used when obtaining Connections from the underlying driver. Note that 
	 * for DataSources this works in a very deterministic fashion by calling DataSource.getConnection(*username*, password) on the underlying
	 * DataSource. However, for Driver-based configurations, every driver is different. In the case of Driver-based, HikariCP will
	 * use this username property to set a user property in the Properties passed to the driver's DriverManager.getConnection(jdbcUrl, props) 
	 * call. If this is not what you need, skip this method entirely and call addDataSourceProperty("username", ...), for example. 
	 * Default: none
	 */
	public static final String USERNAME             = "username";
	
	/**
	 * This property sets the default authentication password used when obtaining Connections from the underlying driver. Note that for 
	 * DataSources this works in a very deterministic fashion by calling DataSource.getConnection(username, *password*) on the underlying 
	 * DataSource. However, for Driver-based configurations, every driver is different. In the case of Driver-based, HikariCP will use this 
	 * password property to set a password property in the Properties passed to the driver's DriverManager.getConnection(jdbcUrl, props) call. 
	 * If this is not what you need, skip this method entirely and call addDataSourceProperty("pass", ...), for example. 
	 * Default: none
	 */
	public static final String PASSWORD             = "password";
	
	/** 
	 * This property controls the default auto-commit behavior of connections returned from the pool. It is a boolean value. 
	 * Default: true
	 * */
	public static final String AUTOCOMMIT           = "autoCommit";
	
	/**
	 * This property controls the maximum number of milliseconds that a client (that's you) will wait for a connection from the pool. 
	 * If this time is exceeded without a connection becoming available, a SQLException will be thrown. 
	 * Lowest acceptable connection timeout is 250 ms. 
	 * 
	 * Default: 30000 (30 seconds)
	 */
	public static final String CONNECTIONTIMEOUT    = "connectionTimeout";
	
	/**
	 * This property controls the maximum amount of time that a connection is allowed to sit idle in the pool. 
	 * This setting only applies when minimumIdle is defined to be less than maximumPoolSize. 
	 * Idle connections will not be retired once the pool reaches minimumIdle connections. 
	 * Whether a connection is retired as idle or not is subject to a maximum variation of +30 seconds, and average variation of +15 seconds. 
	 * A connection will never be retired as idle before this timeout. A value of 0 means that idle connections are never removed from the pool. 
	 * The minimum allowed value is 10000ms (10 seconds). 
	 * Default: 600000 (10 minutes)
	 */
	public static final String IDLETIMEOUT          = "idleTimeout";
	
	/**
	 * This property controls how frequently HikariCP will attempt to keep a connection alive, in order to prevent it from being timed out by the database or network infrastructure. 
	 * This value must be less than the maxLifetime value. A "keepalive" will only occur on an idle connection. 
	 * When the time arrives for a "keepalive" against a given connection, that connection will be removed from the pool, "pinged", and then returned to the pool. 
	 * The 'ping' is one of either: invocation of the JDBC4 isValid() method, or execution of the connectionTestQuery. 
	 * Typically, the duration out-of-the-pool should be measured in single digit milliseconds or even sub-millisecond, and therefore should have little or no noticible performance impact. 
	 * The minimum allowed value is 30000ms (30 seconds), but a value in the range of minutes is most desirable. 
	 * Default: 0 (disabled)
	 */
	public static final String KEEPALIVETIME        = "keepaliveTime";
	
	/**
	 * This property controls the maximum lifetime of a connection in the pool. 
	 * An in-use connection will never be retired, only when it is closed will it then be removed. 
	 * On a connection-by-connection basis, minor negative attenuation is applied to avoid mass-extinction in the pool. 
	 * We strongly recommend setting this value, and it should be several seconds shorter than any database or infrastructure imposed connection time limit. 
	 * A value of 0 indicates no maximum lifetime (infinite lifetime), subject of course to the idleTimeout setting. 
	 * The minimum allowed value is 30000ms (30 seconds). 
	 * Default: 1800000 (30 minutes)
	 */
	public static final String MAXLIFETIME          = "maxLifetime";
	
	/**
	 * If your driver supports JDBC4 we strongly recommend not setting this property. 
	 * This is for "legacy" drivers that do not support the JDBC4 Connection.isValid() API. 
	 * This is the query that will be executed just before a connection is given to you from the pool to validate that the connection to the database is still alive. 
	 * Again, try running the pool without this property, HikariCP will log an error if your driver is not JDBC4 compliant to let you know. 
	 * Default: none
	 */
	public static final String CONNECTIONTESTQUERY = "connectionTestQuery";
	
	/**
	 * This property controls the minimum number of idle connections that HikariCP tries to maintain in the pool. 
	 * If the idle connections dip below this value and total connections in the pool are less than maximumPoolSize, 
	 * HikariCP will make a best effort to add additional connections quickly and efficiently. However, 
	 * for maximum performance and responsiveness to spike demands, we recommend not setting this value and instead allowing HikariCP to act as a fixed size connection pool. 
	 * Default: same as maximumPoolSize
	 */
	public static final String MINIMUMIDLE = "minimumIdle";
	
	/**
	 * This property controls the maximum size that the pool is allowed to reach, including both idle and in-use connections. 
	 * Basically this value will determine the maximum number of actual connections to the database backend. 
	 * A reasonable value for this is best determined by your execution environment. 
	 * When the pool reaches this size, and no idle connections are available, calls to getConnection() will block for up to connectionTimeout milliseconds before timing out. 
	 * Please read about pool sizing. Default: 10
	 */
	public static final String MAXIMUMPOOLSIZE = "maximumPoolSize";
	
	/**
	 * This property represents a user-defined name for the connection pool and appears mainly in logging and JMX management consoles to identify pools and pool configurations. 
	 * Default: auto-generated
	 */
	public static final String POOLNAME = "poolName";

	//// Infrequently used
	/**
	 * This property controls whether the pool will "fail fast" if the pool cannot be seeded with an initial connection successfully. 
	 * Any positive number is taken to be the number of milliseconds to attempt to acquire an initial connection; 
	 * the application thread will be blocked during this period. 
	 * If a connection cannot be acquired before this timeout occurs, an exception will be thrown. 
	 * This timeout is applied after the connectionTimeout period. If the value is zero (0), HikariCP will attempt to obtain and validate a connection. 
	 * If a connection is obtained, but fails validation, an exception will be thrown and the pool not started. 
	 * However, if a connection cannot be obtained, the pool will start, but later efforts to obtain a connection may fail. 
	 * A value less than zero will bypass any initial connection attempt, and the pool will start immediately while trying to obtain connections in the background. 
	 * Consequently, later efforts to obtain a connection may fail. 
	 * Default: 1
	 */
	public static final String INITIALIZATIONFAILTIMEOUT = "initializationFailTimeout";
	
	/**
	 * This property determines whether HikariCP isolates internal pool queries, such as the connection alive test, in their own transaction. 
	 * Since these are typically read-only queries, it is rarely necessary to encapsulate them in their own transaction. 
	 * This property only applies if autoCommit is disabled. 
	 * Default: false
	 */
	public static final String ISOLATEINTERNALQUERIES    = "isolateInternalQueries";
	
	/**
	 * This property controls whether the pool can be suspended and resumed through JMX. This is useful for certain failover automation scenarios. 
	 * When the pool is suspended, calls to getConnection() will not timeout and will be held until the pool is resumed. 
	 * Default: false
	 */
	public static final String ALLOWPOOLSUSPENSION       = "allowPoolSuspension";
	
	/**
	 * This property controls whether Connections obtained from the pool are in read-only mode by default. 
	 * Note some databases do not support the concept of read-only mode, while others provide query optimizations when the Connection is set to read-only. 
	 * Whether you need this property or not will depend largely on your application and database. 
	 * Default: false
	 */
	public static final String READONLY                  = "readOnly";
	
	/**
	 * This property controls whether or not JMX Management Beans ("MBeans") are registered or not. 
	 * Default: false
	 */
	public static final String REGISTERMBEANS            = "registerMbeans";
	
	/**
	 * This property sets the default catalog for databases that support the concept of catalogs. 
	 * If this property is not specified, the default catalog defined by the JDBC driver is used. 
	 * Default: driver default
	 */
	public static final String CATALOG                   = "catalog";
	
	/**
	 * This property sets a SQL statement that will be executed after every new connection creation before adding it to the pool. 
	 * If this SQL is not valid or throws an exception, it will be treated as a connection failure and the standard retry logic will be followed. 
	 * Default: none
	 */
	public static final String CONNECTIONINITSQL         = "connectionInitSql";
	
	/**
	 * HikariCP will attempt to resolve a driver through the DriverManager based solely on the jdbcUrl, but for some older drivers the driverClassName must also be specified. 
	 * Omit this property unless you get an obvious error message indicating that the driver was not found. 
	 * Default: none
	 */
	public static final String DRIVERCLASSNAME = "driverClassName";
	
	/**
	 * This property controls the default transaction isolation level of connections returned from the pool. 
	 * If this property is not specified, the default transaction isolation level defined by the JDBC driver is used. 
	 * Only use this property if you have specific isolation requirements that are common for all queries. 
	 * The value of this property is the constant name from the Connection class such as TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ, etc. 
	 * Default: driver default
	 */
	public static final String TRANSACTIONISOLATION = "transactionIsolation";
	
	/**
	 * This property controls the maximum amount of time that a connection will be tested for aliveness. 
	 * This value must be less than the connectionTimeout. 
	 * Lowest acceptable validation timeout is 250 ms. 
	 * Default: 5000
	 */
	public static final String VALIDATIONTIMEOUT = "validationTimeout";
	
	/**
	 * This property controls the amount of time that a connection can be out of the pool before a message is logged indicating a possible connection leak. 
	 * A value of 0 means leak detection is disabled. Lowest acceptable value for enabling leak detection is 2000 (2 seconds). 
	 * Default: 0
	 */
	public static final String LEAKDETECTIONTHRESHOLD = "leakDetectionThreshold";
	
	/**
	 * This property sets the default schema for databases that support the concept of schemas. 
	 * If this property is not specified, the default schema defined by the JDBC driver is used. 
	 * Default: driver default
	 */
	public static final String SCHEMA = "schema";

	
	private final Properties hikariProperties;
	private final Properties dataSourceProperties;
	
	public HikariCPBuilder() {
		hikariProperties = new Properties();
		dataSourceProperties = new Properties();
	}
	
	public HikariCPBuilder withHikariProperty(String key, String value) {
		hikariProperties.put(key, value);
		return this;
	}

	public HikariCPBuilder withHikariProperties(Properties properties) {
		hikariProperties.putAll(properties);
		return this;
	}

	public HikariCPBuilder withDataSourceProperty(String key, String value) {
		dataSourceProperties.put(key, value);
		return this;
	}

	public HikariCPBuilder withDataSourceProperties(Properties properties) {
		dataSourceProperties.putAll(properties);
		return this;
	}
	
	public HikariCP createDataSource() {
		final HikariDataSource dataSource = new HikariDataSource();
		dataSource.setDataSourceProperties(dataSourceProperties);
		//// Essentials
		// dataSourceClassName 
		if (hikariProperties.containsKey(DATASOURCECLASSNAME)) {
			String dataSourceClassName = hikariProperties.getProperty(DATASOURCECLASSNAME);
			dataSource.setDataSourceClassName(dataSourceClassName);
		}

		// jdbcUrl 
		if (hikariProperties.containsKey(JDBCURL)) {
			String url = hikariProperties.getProperty(JDBCURL);
			dataSource.setJdbcUrl(url);
		}

		// username
		if (hikariProperties.containsKey(USERNAME)) {
			String username = hikariProperties.getProperty(USERNAME);
			dataSource.setUsername(username);
		}

		// password
		if (hikariProperties.containsKey(PASSWORD)) {
			String password = hikariProperties.getProperty(PASSWORD);
			dataSource.setPassword(password);
		}

		//// Frequently used
		// autoCommit
		if (hikariProperties.containsKey(AUTOCOMMIT)) {
			String strAutocommit = hikariProperties.getProperty(AUTOCOMMIT);
			boolean autocommit = Boolean.parseBoolean(strAutocommit);
			dataSource.setAutoCommit(autocommit);
		}
		
		// connectionTimeout
		if (hikariProperties.containsKey(CONNECTIONTIMEOUT)) {
			String strConnectionTimeout = hikariProperties.getProperty(CONNECTIONTIMEOUT);
			try {
				long connectionTimeout = Long.parseLong(strConnectionTimeout);
				dataSource.setConnectionTimeout(connectionTimeout);
			} catch (Exception e) {
			}
		}
		
		// idleTimeout
		if (hikariProperties.containsKey(IDLETIMEOUT)) {
			String strIdleTimeout = hikariProperties.getProperty(IDLETIMEOUT);
			try {
				long idleTimeout = Long.parseLong(strIdleTimeout);
				dataSource.setIdleTimeout(idleTimeout);
			} catch (Exception e) {
			}
		}
		
		// keepaliveTime
		if (hikariProperties.containsKey(KEEPALIVETIME)) {
			String strKeepaliveTime = hikariProperties.getProperty(KEEPALIVETIME);
			try {
				long keepaliveTime = Long.parseLong(strKeepaliveTime);
				dataSource.setKeepaliveTime(keepaliveTime);
			} catch (Exception e) {
			}
		}
		
		// maxLifetime
		if (hikariProperties.containsKey(MAXLIFETIME)) {
			String strMaxLifetime = hikariProperties.getProperty(MAXLIFETIME);
			try {
				long maxLifetime = Long.parseLong(strMaxLifetime);
				dataSource.setMaxLifetime(maxLifetime);
			} catch (Exception e) {
			}
		}
		
		// connectionTestQuery
		if (hikariProperties.containsKey(CONNECTIONTESTQUERY)) {
			String connectionTestQuery = hikariProperties.getProperty(CONNECTIONTESTQUERY);
			dataSource.setConnectionTestQuery(connectionTestQuery);
		}
		
		// minimumIdle
		if (hikariProperties.containsKey(MINIMUMIDLE)) {
			String strMinimumIdle = hikariProperties.getProperty(MINIMUMIDLE);
			try {
				int minimumIdle = Integer.parseInt(strMinimumIdle);
				dataSource.setMinimumIdle(minimumIdle);
			} catch (Exception e) {
			}
		}
		
		// maximumPoolSize
		if (hikariProperties.containsKey(MAXIMUMPOOLSIZE)) {
			String strMaximumPoolSize = hikariProperties.getProperty(MAXIMUMPOOLSIZE);
			try {
				int maximumPoolSize = Integer.parseInt(strMaximumPoolSize);
				dataSource.setMaximumPoolSize(maximumPoolSize);
			} catch (Exception e) {
			}
		}
		
		// poolName
		if (hikariProperties.containsKey(POOLNAME)) {
			String poolName = hikariProperties.getProperty(POOLNAME);
			dataSource.setPoolName(poolName);
		}
		
		//// Infrequently used
		// initializationFailTimeout
		if (hikariProperties.containsKey(INITIALIZATIONFAILTIMEOUT)) {
			String strInitializationFailTimeout = hikariProperties.getProperty(INITIALIZATIONFAILTIMEOUT);
			try {
				Long initializationFailTimeout = Long.parseLong(strInitializationFailTimeout);
				dataSource.setInitializationFailTimeout(initializationFailTimeout);
			} catch (Exception e) {
			}
		}
		
		// isolateInternalQueries
		if (hikariProperties.containsKey(ISOLATEINTERNALQUERIES)) {
			String strIsolateInternalQueries = hikariProperties.getProperty(ISOLATEINTERNALQUERIES);
			try {
				boolean isolateInternalQueries = Boolean.parseBoolean(strIsolateInternalQueries);
				dataSource.setIsolateInternalQueries(isolateInternalQueries);
			} catch (Exception e) {
			}
		}
		
		// allowPoolSuspension
		if (hikariProperties.containsKey(ALLOWPOOLSUSPENSION)) {
			String strAllowPoolSuspension = hikariProperties.getProperty(ALLOWPOOLSUSPENSION);
			try {
				boolean allowPoolSuspension = Boolean.parseBoolean(strAllowPoolSuspension);
				dataSource.setAllowPoolSuspension(allowPoolSuspension);
			} catch (Exception e) {
			}
		}
		
		// readOnly
		if (hikariProperties.containsKey(READONLY)) {
			String strReadOnly = hikariProperties.getProperty(READONLY);
			try {
				boolean readOnly = Boolean.parseBoolean(strReadOnly);
				dataSource.setReadOnly(readOnly);
			} catch (Exception e) {
			}
		}
		
		// registerMbeans
		if (hikariProperties.containsKey(REGISTERMBEANS)) {
			String strRegisterMbeans = hikariProperties.getProperty(REGISTERMBEANS);
			try {
				boolean registerMbeans = Boolean.parseBoolean(strRegisterMbeans);
				dataSource.setRegisterMbeans(registerMbeans);
			} catch (Exception e) {
			}
		}
		
		// catalog
		if (hikariProperties.containsKey(CATALOG)) {
			String catalog = hikariProperties.getProperty(CATALOG);
			dataSource.setCatalog(catalog);
		}
		
		// connectionInitSql
		if (hikariProperties.containsKey(CONNECTIONINITSQL)) {
			String connectionInitSql = hikariProperties.getProperty(CONNECTIONINITSQL);
			dataSource.setConnectionInitSql(connectionInitSql);
		}
		
		// driverClassName
		if (hikariProperties.containsKey(DRIVERCLASSNAME)) {
			String driverClassName = hikariProperties.getProperty(DRIVERCLASSNAME);
			dataSource.setDriverClassName(driverClassName);
		}
		
		// transactionIsolation
		if (hikariProperties.containsKey(TRANSACTIONISOLATION)) {
			String transactionIsolation = hikariProperties.getProperty(TRANSACTIONISOLATION);
			dataSource.setTransactionIsolation(transactionIsolation);
		}
		
		// validationTimeout
		if (hikariProperties.containsKey(VALIDATIONTIMEOUT)) {
			String strValidationTimeout = hikariProperties.getProperty(VALIDATIONTIMEOUT);
			try {
				Long validationTimeout = Long.parseLong(strValidationTimeout);
				dataSource.setValidationTimeout(validationTimeout);
			} catch (Exception e) {
			}
		}
		
		// leakDetectionThreshold
		if (hikariProperties.containsKey(LEAKDETECTIONTHRESHOLD)) {
			String strLeakDetectionThreshold = hikariProperties.getProperty(LEAKDETECTIONTHRESHOLD);
			try {
				Long leakDetectionThreshold = Long.parseLong(strLeakDetectionThreshold);
				dataSource.setLeakDetectionThreshold(leakDetectionThreshold);
			} catch (Exception e) {
			}
		}
		
		// schema
		if (hikariProperties.containsKey(SCHEMA)) {
			String schema = hikariProperties.getProperty(SCHEMA);
			dataSource.setSchema(schema);
		}
		
		return new HikariCP(dataSource);
	}
	
}
