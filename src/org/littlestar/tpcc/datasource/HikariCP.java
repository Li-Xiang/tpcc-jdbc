package org.littlestar.tpcc.datasource;

import java.sql.Connection;
import com.zaxxer.hikari.HikariDataSource;

public class HikariCP implements TpccDataSource {
	private final HikariDataSource dataSource;

	public HikariCP(HikariDataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Override
	public Connection getConnection() throws Exception {
		return dataSource.getConnection();
	}
	
	@Override
	public void close() throws Exception {
		dataSource.close();
	}
	
	@Override
	public String getDataSourceClassName() {
		return HikariDataSource.class.getCanonicalName();
	}

}
