package org.littlestar.tpcc.datasource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class NonPoolDataSource implements TpccDataSource {
	private Properties info;
	private String url = null;
	
	public NonPoolDataSource(String url) {
		this.url = url;
		info = new Properties();
	}
	
	public NonPoolDataSource(String url, String driver) throws Exception {
		this(url);
		setDriver(driver);
	}
	
	public void setConnectionProperty(String key, String value) {
		info.setProperty(key, value);
		
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
	
	public void setUser(String user) {
		setConnectionProperty("user", user);
	}
	
	public void setPassword(String password) {
		setConnectionProperty("password", password);
	}
	
	public void setDriver(String driver) throws Exception {
		Class.forName(driver);
	}

	@Override
	public Connection getConnection() throws Exception {
		return DriverManager.getConnection(url, info);
	}
	
	@Override
	public void close() throws Exception {}

	@Override
	public String getDataSourceClassName() {
		return NonPoolDataSource.class.getCanonicalName();
	}
}
