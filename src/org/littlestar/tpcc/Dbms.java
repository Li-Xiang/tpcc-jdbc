package org.littlestar.tpcc;

import java.util.Objects;

public enum Dbms {
	MySQL, Oracle, Derby, SQLite, MSSQL, PostgreSQL, DB2, H2, Unknown;
	//public static final String MYSQL  = "MySQL";
	//public static final String MSSQL  = "MSSQL";
	//public static final String SQLITE = "SQLite";
	public static final String MYSQL_CJ_DRIVER = "com.mysql.cj.jdbc.Driver";
	private Dbms() {}

	public static Dbms parse(String name) {
		if (Objects.isNull(name)) {
			return Unknown;
		}
		Dbms[] dbmsArray = Dbms.values();
		for (Dbms dbms : dbmsArray) {
			if (dbms.name().equalsIgnoreCase(name)) {
				return dbms;
			}
		}
		return Unknown;
	}
}
