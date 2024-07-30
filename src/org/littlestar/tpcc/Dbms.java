package org.littlestar.tpcc;

import java.util.Objects;

public enum Dbms {
	MySQL, Oracle, Derby, SQLite, MSSQL, PostgreSQL, DB2, H2, OB_Oracle, OB_MySQL, DM, OpenGauss, Unknown;

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
