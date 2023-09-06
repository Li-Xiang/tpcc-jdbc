package org.littlestar.tpcc.datasource;

import java.sql.Connection;

public interface TpccDataSource {
	public Connection getConnection() throws Exception;
	public void close() throws Exception;
	public String getDataSourceClassName();
}
