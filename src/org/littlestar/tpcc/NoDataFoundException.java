package org.littlestar.tpcc;

import java.sql.SQLException;

public class NoDataFoundException extends SQLException {
	private static final long serialVersionUID = -1657369560909425059L;

	public NoDataFoundException(String reason) {
		super(reason);
	}
}
