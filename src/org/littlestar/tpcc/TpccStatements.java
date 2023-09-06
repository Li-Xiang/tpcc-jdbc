package org.littlestar.tpcc;

import java.util.Objects;

public final class TpccStatements {

	private TpccStatements() {}
	
	public static String createItemSQL(Dbms dbms) {
		String sqlText = 
				"create table item ( " + 
		        "  i_id int not null," + 
				"  i_im_id int," + 
		        "  i_name varchar(24)," + 
				"  i_price decimal(5,2), " + 
		        "  i_data varchar(50) " + 
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle = 
				"create table item ( "+
				"  i_id int not null, "+ 
				"  i_im_id int, "+
				"  i_name varchar2(24), "+
				"  i_price decimal(5,2), "+
				"  i_data varchar2(50) "+
				")";
		switch(dbms){
		case MySQL: return mysql;
		case Oracle: return oracle;
		default: return sqlText;
		}
	}
	
	public static String createWarehouseSQL(Dbms dbms) {
		String sqlText = 
				"create table warehouse ( "+
				"  w_id smallint not null, "+
				"  w_name varchar(10), "+
				"  w_street_1 varchar(20), "+ 
				"  w_street_2 varchar(20), "+ 
				"  w_city varchar(20), "+ 
				"  w_state char(2), "+ 
				"  w_zip char(9), "+ 
				"  w_tax decimal(4,4), "+ 
				"  w_ytd decimal(12,2) "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle = 
				"create table warehouse ( "+
				"  w_id smallint not null, "+
				"  w_name varchar2(10), "+ 
				"  w_street_1 varchar2(20), "+ 
				"  w_street_2 varchar2(20), "+ 
				"  w_city varchar2(20), "+ 
				"  w_state char(2), "+ 
				"  w_zip char(9), "+ 
				"  w_tax decimal(4,2), "+ 
				"  w_ytd decimal(12,2) "+
				")";
		
		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		default:     return sqlText;
		}
	}
	
	public static String createStock(Dbms dbms) {
		String sqlText = 
				"create table stock ( "+
				"  s_i_id int not null, "+
				"  s_w_id smallint not null, "+
				"  s_quantity smallint, "+
				"  s_dist_01 char(24), "+
				"  s_dist_02 char(24), "+
				"  s_dist_03 char(24), "+
				"  s_dist_04 char(24), "+
				"  s_dist_05 char(24), "+
				"  s_dist_06 char(24), "+
				"  s_dist_07 char(24), "+
				"  s_dist_08 char(24), "+
				"  s_dist_09 char(24), "+
				"  s_dist_10 char(24), "+
				"  s_ytd decimal(8,0), "+
				"  s_order_cnt smallint, "+
				"  s_remote_cnt smallint, "+
				"  s_data varchar(50) "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle = 
				"create table stock( "+
				"  s_i_id int not null, "+
				"  s_w_id smallint not null, "+ 
				"  s_quantity smallint, "+
				"  s_dist_01 char(24), "+
				"  s_dist_02 char(24), "+
				"  s_dist_03 char(24), "+
				"  s_dist_04 char(24), "+
				"  s_dist_05 char(24), "+
				"  s_dist_06 char(24), "+
				"  s_dist_07 char(24), "+
				"  s_dist_08 char(24), "+
				"  s_dist_09 char(24), "+
				"  s_dist_10 char(24), "+
				"  s_ytd decimal(8,0), "+
				"  s_order_cnt smallint, "+
				"  s_remote_cnt smallint, "+
				"  s_data varchar2(50) "+
				")";

		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		default:     return sqlText;
		}
	}
	
	public static String createDistrictSQL(Dbms dbms) {
		String sqlText = "create table district( "+
				"  d_id tinyint not null, "+ 
				"  d_w_id smallint not null, "+ 
				"  d_name varchar(10), "+ 
				"  d_street_1 varchar(20), "+ 
				"  d_street_2 varchar(20), "+ 
				"  d_city varchar(20), "+ 
				"  d_state char(2), "+ 
				"  d_zip char(9), "+ 
				"  d_tax decimal(4,4), "+ 
				"  d_ytd decimal(12,2), "+ 
				"  d_next_o_id int "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle = 
				"create table district ( "+
				"  d_id number(3,0) not null, "+
				"  d_w_id smallint not null, "+
				"  d_name varchar2(10), "+
				"  d_street_1 varchar2(20), "+
				"  d_street_2 varchar2(20), "+
				"  d_city varchar2(20), "+
				"  d_state char(2), "+
				"  d_zip char(9), "+
				"  d_tax decimal(4,2), "+
				"  d_ytd decimal(12,2), "+
				"  d_next_o_id int "+
				")";
		
		String pgsql = "create table district( "+
				"  d_id smallint not null, "+ 
				"  d_w_id smallint not null, "+ 
				"  d_name varchar(10), "+ 
				"  d_street_1 varchar(20), "+ 
				"  d_street_2 varchar(20), "+ 
				"  d_city varchar(20), "+ 
				"  d_state char(2), "+ 
				"  d_zip char(9), "+ 
				"  d_tax decimal(4,4), "+ 
				"  d_ytd decimal(12,2), "+ 
				"  d_next_o_id int "+
				")";
		
		String db2 = pgsql;
		String h2 = pgsql;
		String derby = db2;
		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		case PostgreSQL : return pgsql;
		case DB2 : return db2;
		case H2 : return h2; 
		case Derby:  return derby;
		default:     return sqlText;
		}
	}
	
	public static String createCustomerSQL(Dbms dbms) {
		String sqlText = 
				"create table customer ( "+
				"  c_id int not null, "+ 
				"  c_d_id tinyint not null, "+
				"  c_w_id smallint not null, "+ 
				"  c_first varchar(16), "+
				"  c_middle char(2), "+
				"  c_last varchar(16), "+
				"  c_street_1 varchar(20), "+
				"  c_street_2 varchar(20), "+
				"  c_city varchar(20), "+
				"  c_state char(2), "+
				"  c_zip char(9), "+
				"  c_phone char(16), "+
				"  c_since datetime, "+
				"  c_credit char(2), "+
				"  c_credit_lim bigint, "+
				"  c_discount decimal(4,4), "+
				"  c_balance decimal(12,2), "+
				"  c_ytd_payment decimal(12,2), "+
				"  c_payment_cnt smallint, "+
				"  c_delivery_cnt smallint, "+
				"  c_data text "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle =
				"create table customer ( "+
				"  c_id number(10,0) not null,  "+
				"  c_d_id number(3,0) not null, "+ 
				"  c_w_id smallint not null, "+ 
				"  c_first varchar2(16), "+ 
				"  c_middle char(2), "+ 
				"  c_last varchar2(16), "+ 
				"  c_street_1 varchar2(20), "+ 
				"  c_street_2 varchar2(20), "+ 
				"  c_city varchar2(20), "+
				"  c_state char(2), "+ 
				"  c_zip char(9), "+ 
				"  c_phone char(16), "+ 
				"  c_since date, "+ 
				"  c_credit char(2), "+ 
				"  c_credit_lim number(24,0), "+ 
				"  c_discount decimal(4,2), "+ 
				"  c_balance decimal(12,2), "+ 
				"  c_ytd_payment decimal(12,2), "+ 
				"  c_payment_cnt smallint, "+ 
				"  c_delivery_cnt smallint, "+ 
				"  c_data clob "+
				")";
		String pgsql = 
				"create table customer ( "+
				"  c_id int not null, "+ 
				"  c_d_id smallint not null, "+
				"  c_w_id smallint not null, "+ 
				"  c_first varchar(16), "+
				"  c_middle char(2), "+
				"  c_last varchar(16), "+
				"  c_street_1 varchar(20), "+
				"  c_street_2 varchar(20), "+
				"  c_city varchar(20), "+
				"  c_state char(2), "+
				"  c_zip char(9), "+
				"  c_phone char(16), "+
				"  c_since timestamp without time zone, "+
				"  c_credit char(2), "+
				"  c_credit_lim bigint, "+
				"  c_discount decimal(4,4), "+
				"  c_balance decimal(12,2), "+
				"  c_ytd_payment decimal(12,2), "+
				"  c_payment_cnt smallint, "+
				"  c_delivery_cnt smallint, "+
				"  c_data text "+
				")";
		String db2 = 				
				"create table customer ( "+
				"  c_id int not null, "+ 
				"  c_d_id smallint not null, "+
				"  c_w_id smallint not null, "+ 
				"  c_first varchar(16), "+
				"  c_middle char(2), "+
				"  c_last varchar(16), "+
				"  c_street_1 varchar(20), "+
				"  c_street_2 varchar(20), "+
				"  c_city varchar(20), "+
				"  c_state char(2), "+
				"  c_zip char(9), "+
				"  c_phone char(16), "+
				"  c_since timestamp, "+
				"  c_credit char(2), "+
				"  c_credit_lim bigint, "+
				"  c_discount decimal(4,4), "+
				"  c_balance decimal(12,2), "+
				"  c_ytd_payment decimal(12,2), "+
				"  c_payment_cnt smallint, "+
				"  c_delivery_cnt smallint, "+
				"  c_data clob "+
				")";
		String h2 = pgsql;
		String derby = db2;
		
		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		case PostgreSQL: return pgsql;
		case DB2: return db2;
		case H2: return h2;
		case Derby:  return derby;
		default:     return sqlText;
		}
	}

	public static String createHistorySQL(Dbms dbms) {
		String sqlText = 
				"create table history ( "+
				"  h_c_id int, "+
				"  h_c_d_id tinyint, "+
				"  h_c_w_id smallint, "+
				"  h_d_id tinyint, "+
				"  h_w_id smallint, "+
				"  h_date datetime, "+
				"  h_amount decimal(6,2), "+
				"  h_data varchar(24) "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle = 
				"create table history ( "+
				"  h_c_id int, "+
				"  h_c_d_id number(3,0), "+ 
				"  h_c_w_id smallint, "+
				"  h_d_id number(3,0), "+
				"  h_w_id smallint, "+
				"  h_date date, "+
				"  h_amount decimal(6,2), "+ 
				"  h_data varchar2(24) "+
				")";
		String pgsql = 
				"create table history ( "+
				"  h_c_id int, "+
				"  h_c_d_id smallint, "+
				"  h_c_w_id smallint, "+
				"  h_d_id smallint, "+
				"  h_w_id smallint, "+
				"  h_date timestamp without time zone, "+
				"  h_amount decimal(6,2), "+
				"  h_data varchar(24) "+
				")";
		String db2 = 
				"create table history ( "+
				"  h_c_id int, "+
				"  h_c_d_id smallint, "+
				"  h_c_w_id smallint, "+
				"  h_d_id smallint, "+
				"  h_w_id smallint, "+
				"  h_date timestamp, "+
				"  h_amount decimal(6,2), "+
				"  h_data varchar(24) "+
				")";
		String h2 = pgsql;
		String derby = db2;
		
		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		case PostgreSQL: return pgsql;
		case DB2: return db2;
		case H2: return h2;
		case Derby: return derby;
		default:     return sqlText;
		}
	}

	public static String createOrdersSQL(Dbms dbms) {
		String sqlText = 
				"create table orders ( "+
				"  o_id int not null, "+ 
				"  o_d_id tinyint not null, "+ 
				"  o_w_id smallint not null, "+
				"  o_c_id int, "+
				"  o_entry_d datetime, "+
				"  o_carrier_id tinyint, "+
				"  o_ol_cnt tinyint, "+ 
				"  o_all_local tinyint "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle = "create table orders ( "+
				"  o_id int not null, "+ 
				"  o_d_id number(3,0) not null, "+ 
				"  o_w_id smallint not null, "+
				"  o_c_id int, "+
				"  o_entry_d date, "+
				"  o_carrier_id number(3,0), "+
				"  o_ol_cnt number(3,0), "+ 
				"  o_all_local number(3,0) "+
				")";
		String pgsql = 
				"create table orders ( "+
				"  o_id int not null, "+ 
				"  o_d_id smallint not null, "+ 
				"  o_w_id smallint not null, "+
				"  o_c_id int, "+
				"  o_entry_d timestamp without time zone, "+
				"  o_carrier_id smallint, "+
				"  o_ol_cnt smallint, "+ 
				"  o_all_local smallint "+
				")"; 
		String db2 = 
				"create table orders ( "+
				"  o_id int not null, "+ 
				"  o_d_id smallint not null, "+ 
				"  o_w_id smallint not null, "+
				"  o_c_id int, "+
				"  o_entry_d timestamp, "+
				"  o_carrier_id smallint, "+
				"  o_ol_cnt smallint, "+ 
				"  o_all_local smallint "+
				")"; 
		String h2 = pgsql;
		String derby = db2;
		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		case PostgreSQL: return pgsql;
		case DB2: return db2;
		case H2: return h2;
		case Derby: return derby;
		default:     return sqlText;
		}
	}
	
	public static String createNewOrderSQL(Dbms dbms) {
		String sqlText =
				"create table new_order ( "+
				"  no_o_id int not null, "+
				"  no_d_id tinyint not null, "+
				"  no_w_id smallint not null "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle = "create table new_order ( "+
				"  no_o_id int not null, "+
				"  no_d_id number(3,0) not null, "+
				"  no_w_id smallint not null "+
				")";
		String pgsql =
				"create table new_order ( "+
				"  no_o_id int not null, "+
				"  no_d_id smallint not null, "+
				"  no_w_id smallint not null "+
				")";
		String db2 = pgsql;
		String h2 = pgsql;
		String derby = db2;
		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		case PostgreSQL: return pgsql;
		case DB2:    return db2;
		case H2: return h2;
		case Derby:  return derby;
		default:     return sqlText;
		}
	}
	
	public static String createOrderLineSQL(Dbms dbms) {
		String sqlText = 
				"create table order_line ( "+
				"  ol_o_id int not null, "+ 
				"  ol_d_id tinyint not null, "+
				"  ol_w_id smallint not null, "+
				"  ol_number tinyint not null, "+
				"  ol_i_id int, "+ 
				"  ol_supply_w_id smallint, "+
				"  ol_delivery_d datetime, "+ 
				"  ol_quantity tinyint, "+ 
				"  ol_amount decimal(6,2), "+ 
				"  ol_dist_info char(24) "+
				")";
		String mysql = sqlText + " Engine=InnoDB";
		String oracle =
				"create table order_line ( "+
				"  ol_o_id int not null, "+ 
				"  ol_d_id number(3,0) not null, "+
				"  ol_w_id smallint not null, "+
				"  ol_number  number(3,0) not null, "+
				"  ol_i_id int, "+ 
				"  ol_supply_w_id smallint, "+
				"  ol_delivery_d date, "+ 
				"  ol_quantity number(3,0), "+ 
				"  ol_amount decimal(6,2), "+ 
				"  ol_dist_info char(24) "+
				")";
		String pgsql = 
				"create table order_line ( "+
				"  ol_o_id int not null, "+ 
				"  ol_d_id smallint not null, "+
				"  ol_w_id smallint not null, "+
				"  ol_number smallint not null, "+
				"  ol_i_id int, "+ 
				"  ol_supply_w_id smallint, "+
				"  ol_delivery_d timestamp without time zone, "+ 
				"  ol_quantity smallint, "+ 
				"  ol_amount decimal(6,2), "+ 
				"  ol_dist_info char(24) "+
				")";
		String db2 = 
				"create table order_line ( "+
				"  ol_o_id int not null, "+ 
				"  ol_d_id smallint not null, "+
				"  ol_w_id smallint not null, "+
				"  ol_number smallint not null, "+
				"  ol_i_id int, "+ 
				"  ol_supply_w_id smallint, "+
				"  ol_delivery_d timestamp, "+ 
				"  ol_quantity smallint, "+ 
				"  ol_amount decimal(6,2), "+ 
				"  ol_dist_info char(24) "+
				")";
		String derby = db2;
		switch (dbms) {
		case MySQL:  return mysql;
		case Oracle: return oracle;
		case PostgreSQL: return pgsql;
		case DB2 : return db2;
		case Derby: return derby;
		default:     return sqlText;
		}
	}
	
	////// TPC-C数据加载相关语句  //////
	public static String insertItemSQL(Dbms dbms) {
		String sqlText  = "insert into item values (?,?,?,?,?)";
		return sqlText;
	}
	
	public static String insertWarehouseSQL(Dbms dbms) {
		String sqlText = "insert into warehouse values (?,?,?,?,?,?,?,?,?)";
		return sqlText;
	}
	
	public static String insertStockSQL(Dbms dbms) {
		String sqlText  = "insert into stock values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		return sqlText;
	}
	
	public static String insertDistrictSQL(Dbms dbms) {
		String sqlText  = "insert into district values (?,?,?,?,?,?,?,?,?,?,?)";
		return sqlText;
	}
	
	public static String insertCustomerSQL(Dbms dbms) {
		String sqlText = "insert into customer values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		return sqlText;
	}
	
	public static String insertHistorySQL(Dbms dbms) {
		String sqlText  = "insert into history values (?,?,?,?,?,?,?,?)";
		return sqlText;
	}
	
	public static String insertOrdersSQL(Dbms dbms) {
		String sqlText  = "insert into orders values (?,?,?,?,?,?,?,?)";
		return sqlText;
	}
	
	public static String insertNewOrdersSQL(Dbms dbms) {
		return "insert into new_order values (?,?,?)";
	}
	
	
	public static String insertOrderLineSQL(Dbms dbms) {
		String sqlText = "insert into order_line values (?,?,?,?,?,?,?,?,?,?)";
		return sqlText;
	}
	
	//////  TPCC事务相关语句  //////
	
	//// A.1 The New-Order Transaction
	/**
	 * SELECT c_discount, c_last, c_credit, w_tax 
     *     INTO :c_discount, :c_last, :c_credit, :w_tax
     * FROM customer, warehouse
     * WHERE w_id = :w_id AND c_w_id = w_id AND
     *       c_d_id = :d_id AND c_id = :c_id;
	 */
	public static String newOrderStmt1(Dbms dbms) {
		String sqlText = "select c_discount, c_last, c_credit, w_tax from customer, warehouse  "
				+ "where  w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?";
		return sqlText;
	}
	
	/**
	 * SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
	 * FROM district
	 * WHERE d_id = :d_id AND d_w_id = :w_id
	 * FOR UPDATE;
	 */
	public static String newOrderStmt2(Dbms dbms) {
		if (Objects.equals(dbms, Dbms.MSSQL)) {
			// MSSQL not support select ... for update.
			String sqlTextMssql = "select d_next_o_id, d_tax from district with (UPDLOCK, INDEX(idx_district_1)) where d_id = ? and d_w_id = ?";
			return sqlTextMssql;
		} else if (Objects.equals(dbms, Dbms.SQLite)) {
			// SQLite也不支持select ... for update语法, 也不支持加行锁定.
			String sqlTextSqlite = "select d_next_o_id, d_tax from district where d_id = ? and d_w_id = ?";
			return sqlTextSqlite;
		} else {
			String sqlText = "select d_next_o_id, d_tax from district where d_id = ? and d_w_id = ? for update";
			return sqlText;
		}
	}
	
	/**
	 * UPDATE district SET d_next_o_id = :d_next_o_id + 1
	 * WHERE d_id = :d_id AND d_w_id = :w_id;
	 */
	public static String newOrderStmt3(Dbms dbms) {
		String sqlText = "update district set d_next_o_id = ? + 1 where d_id = ? and d_w_id = ?";
		return sqlText;
	}
	
	/**
	 * INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
	 * VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	 */
	public static String newOrderStmt4(Dbms dbms) {
		String sqlText = "insert into orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) values (?,?,?,?,?,?,?)";
		return sqlText;
	}

	/**
	 * INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
	 * VALUES (:o_id, :d_id, :w_id);
	 */
	public static String newOrderStmt5(Dbms dbms) {
		String sqlText = "insert into new_order (no_o_id, no_d_id, no_w_id) values (?,?,?)";
		return sqlText;
	}
	
	/**
	 * SELECT i_price, i_name , i_data 
	 * INTO :i_price, :i_name, :i_data
	 * FROM item
	 * WHERE i_id = :ol_i_id;
	 */
	public static String newOrderStmt6(Dbms dbms) {
		String sqlText = "select i_price, i_name, i_data from item where i_id = ?";
		return sqlText;
	}
	
	/**
	 * SELECT s_quantity, s_data, s_dist_01, s_dist_02,
	 *   s_dist_03, s_dist_04, s_dist_05, s_dist_06,
	 *   s_dist_07, s_dist_08, s_dist_09, s_dist_10
	 * INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
	 *   :s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
	 *   :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
	 * FROM stock
	 * WHERE s_i_id = :ol_i_id 
	 * AND s_w_id = :ol_supply_w_id
	 * FOR UPDATE
	 */
	public static String newOrderStmt7(Dbms dbms) {
		if (Objects.equals(dbms, Dbms.MSSQL)) {
			String sqlTextMssql = "select s_quantity, s_data, s_dist_01, s_dist_02,"
					+ " s_dist_03, s_dist_04, s_dist_05, s_dist_06,"
					+ " s_dist_07, s_dist_08, s_dist_09, s_dist_10 "
					+ "from stock with (UPDLOCK, INDEX(idx_stock_1)) "
					+ "where s_i_id = ? and s_w_id = ?";
			return sqlTextMssql;
		} else if (Objects.equals(dbms, Dbms.SQLite)) {
			String sqlTextSqlite = "select s_quantity, s_data, s_dist_01, s_dist_02,"
					+ " s_dist_03, s_dist_04, s_dist_05, s_dist_06,"
					+ " s_dist_07, s_dist_08, s_dist_09, s_dist_10 "
					+ "from stock "
					+ "where s_i_id = ? and s_w_id = ?";
			return sqlTextSqlite;
		} else {
			String sqlText = "select s_quantity, s_data, s_dist_01, s_dist_02,"
					+ " s_dist_03, s_dist_04, s_dist_05, s_dist_06,"
					+ " s_dist_07, s_dist_08, s_dist_09, s_dist_10 "
					+ "from stock "
					+ "where s_i_id = ? and s_w_id = ? for update";
			return sqlText;
		}
	}
	
	/**
	 * UPDATE stock SET s_quantity = :s_quantity
	 * WHERE s_i_id = :ol_i_id
	 * AND s_w_id = :ol_supply_w_id;
	 */
	public static String newOrderStmt8(Dbms dbms) {
		String sqlText = "update stock set s_quantity = ? where s_i_id = ? and s_w_id = ?";
		return sqlText;
	}
	
	/**
	 * INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,ol_i_id, 
	 *   ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) 
	 * VALUES (:o_id, :d_id, :w_id, :ol_number,:ol_i_id, :ol_supply_w_id, 
	 *   :ol_quantity, :ol_amount, :ol_dist_info);
	 */
	public static String newOrderStmt9(Dbms dbms) {
		String sqlText = "insert into order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, "
				+ "ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) values (?,?,?,?,?,?,?,?,?)";
		return sqlText;
	}

	
	
	////// A.2	The Payment Transaction
	/**
	 * UPDATE warehouse SET w_ytd = w_ytd + :h_amount WHERE w_id=:w_id;
	 */
	public static String paymentStmt1(Dbms dbms) {
		String sqlText = "update warehouse set w_ytd = w_ytd + ?  where w_id = ? ";
		return sqlText;
	}
	
	/**
	 * SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
	 * INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
	 * FROM warehouse
	 * WHERE w_id=:w_id;
	 */
	
	public static String paymentStmt2(Dbms dbms) {
		String sqlText = "select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id=?";
		return sqlText;
	}
	
	/**
	 * 
	 * UPDATE district SET d_ytd = d_ytd + :h_amount 
	 * WHERE d_w_id=:w_id AND d_id=:d_id;
	 * 
	 * @return
	 */
	public static String paymentStmt3(Dbms dbms) {
		String sqlText = "update district set d_ytd = d_ytd + ? where d_w_id = ? and d_id = ? ";
		return sqlText;
	}
	
	
	/**
	 * SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
	 * INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
	 * FROM district
	 * WHERE d_w_id=:w_id AND d_id=:d_id;
	 */
	public static String paymentStmt4(Dbms dbms) {
		String sqlText = "select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where d_w_id = ? and d_id = ?";
		return sqlText;
	}
	
	/**
	 * SELECT count(c_id) INTO :namecnt  FROM customer
	 * WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
	 */
	public static String paymentStmt5(Dbms dbms) {
		String sqlText = "select count(c_id) as namecnt from customer where c_last = ? and c_d_id = ? and c_w_id = ?";
		return sqlText;
	}
	
	/**
	 * SELECT c_first, c_middle, c_id,
	 * c_street_1, c_street_2, c_city, c_state, c_zip, 
	 * c_phone, c_credit, c_credit_lim,
	 * c_discount, c_balance, c_since
	 * FROM customer
	 * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
	 * ORDER BY c_first;
	 */
	public static String paymentStmt6(Dbms dbms) {
		String sqlText = "select c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, "
				+ "c_credit, c_credit_lim, c_discount, c_balance, c_since "
				+ "from customer "
				+ "where c_w_id = ? and c_d_id = ? and c_last = ? order by c_first";
		return sqlText;
	}
	
	/** 
	 * SELECT c_first, c_middle, c_last,
	 *   c_street_1, c_street_2, c_city, c_state, c_zip, 
	 *   c_phone, c_credit, c_credit_lim,
	 *   c_discount, c_balance, c_since
	 *   INTO :c_first, :c_middle, :c_last,
	 *   :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
	 *   :c_phone, :c_credit, :c_credit_lim,
	 *   :c_discount, :c_balance, :c_since
	 * FROM customer
	 * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
	 */
	public static String paymentStmt7(Dbms dbms) {
		String sqlText = "select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, "
				+ "c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since "
				+ "from customer "
				+ "where c_w_id = ? and c_d_id = ? and c_id = ?";
		return sqlText;
	}
	
	/**
	 * SELECT c_data INTO :c_data 
	 * FROM customer
	 * WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
	 */
	public static String paymentStmt8(Dbms dbms) {
		String sqlText = "select c_data from customer where c_w_id = ? and c_d_id = ?  and c_id = ?";
		return sqlText;
	}
	
	/**
	 * UPDATE customer
	 * SET c_balance = :c_balance,  c_data = :c_new_data
	 * WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND
	 * c_id = :c_id;
	 */
	public static String paymentStmt9(Dbms dbms) {
		String sqlText = "update customer set c_balance= ?, c_data = ? where c_w_id= ? and c_d_id= ? and c_id= ?";
		return sqlText;
	}
	
	/**
	 * UPDATE customer SET c_balance = :c_balance
	 * WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND
	 * c_id = :c_id;
	 */
	public static String paymentStmt10(Dbms dbms) {
		final String sqlText = "update customer set c_balance = ? where c_w_id = ? and c_d_id = ? and c_id = ?";
		return sqlText;
	}
	
	/**
	 * INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id,
	 *  h_w_id, h_date, h_amount, h_data) 
	 * VALUES (:c_d_id, :c_w_id, :c_id, :d_id,
	 *  :w_id, :datetime, :h_amount, :h_data); 
	 */
	
	public static String paymentStmt11(Dbms dbms) {
		String sqlText = "insert into history (h_c_d_id, h_c_w_id, h_c_id, h_d_id,h_w_id, h_date, h_amount, h_data) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?)";
		return sqlText;
	}
	
	
	////// Order-Status
	
	/**
	 * SELECT count(c_id) INTO :namecnt
	 * FROM customer
	 * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
	 */
	public static String orderStatusStmt1(Dbms dbms) {
		String sqlText = "select count(c_id) as namecnt from customer where c_last = ? and c_d_id = ? and c_w_id = ?";
		return sqlText;
	}
	
	/**
	 * SELECT c_balance, c_first, c_middle, c_id
	 * FROM customer
	 * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
	 * ORDER BY c_first;
	 */
	public static String orderStatusStmt2(Dbms dbms) {
		String sqlText = "select c_balance, c_first, c_middle, c_id "
				+ "from customer "
				+ "where c_last = ? and c_d_id = ? and c_w_id = ? " 
				+ "order by c_first";
		return sqlText;
	}
	
	
	/**
	 * SELECT c_balance, c_first, c_middle, c_last
	 *   INTO :c_balance, :c_first, :c_middle, :c_last 
	 * FROM customer
	 * WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
	 */
	public static String orderStatusStmt3(Dbms dbms) {
		String sqlText = "select c_balance, c_first, c_middle, c_last "
				+ "from customer "
				+ "where c_id = ? and c_d_id = ? and c_w_id = ?";
		return sqlText;
	}
	
	
	/**
	 * SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0)
	 *   INTO :o_id, :o_entry_d, :o_carrier_id
	 * FROM orders
	 * WHERE o_w_id = :c_w_id
	 *   AND o_d_id = :c_d_id
	 *   AND o_c_id = :c_id
	 *   AND o_id = (SELECT MAX(o_id) FROM orders
	 *     WHERE o_w_id = :c_w_id
	 *           AND o_d_id = :c_d_id AND o_c_id = :c_id);
	 */
	public static String orderStatusStmt4(Dbms dbms) {
		String sqlText = "select o_id, o_entry_d, coalesce(o_carrier_id, 0) as o_carrier_id "
				+ " from orders"
				+ " where o_w_id = ? "
				+ "  and o_d_id = ? "
				+ "  and o_c_id = ? "
				+ "  and o_id = "
				+ "(select max(o_id) from orders where o_w_id = ? and o_d_id = ? and o_c_id =?)";
		return sqlText;
	}
	
    /**
     * SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
     * FROM order_line
     * WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
     */
	public static String orderStatusStmt5(Dbms dbms) {
		String sqlText = "select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
				+ "from order_line "
				+ "where ol_o_id = ? and ol_d_id = ? and ol_w_id = ?";
		return sqlText;
	}
	
	
	////// Delivery
	
	/**
	 * 
	 * SELECT coalesce(min(no_o_id),0) as no_o_id
	 * FROM new_order
	 * WHERE no_d_id = :d_id AND no_w_id = :w_id ;
	 */
	public static String deliveryStmt1(Dbms dbms) {
		String sqlText = "select coalesce(min(no_o_id),0) as no_o_id from new_order where no_d_id = ? and no_w_id = ?";
		return sqlText;
	}
	
	/**
	 * DELETE FROM new_order WHERE no_d_id = ? AND no_w_id = ? AND no_o_id = ?"
	 */
	public static String deliveryStmt2(Dbms dbms) {
		String sqlText = "delete from new_order where no_d_id = ? and no_w_id = ? and no_o_id = ? ";
		return sqlText;
	}
	
	/**
	 * SELECT o_c_id INTO :c_id FROM orders
	 * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
	 * 
	 * @return
	 */
	public static String deliveryStmt3(Dbms dbms) {
		String sqlText = "select o_c_id as c_id from orders where o_id = ? and o_d_id = ? and o_w_id = ?";
		return sqlText;
	}
	
	/**
	 * UPDATE orders SET o_carrier_id = :o_carrier_id
	 * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;  
	 * @return
	 */
	public static String deliveryStmt4(Dbms dbms) {
		String sqlText = "update orders set o_carrier_id = ? where o_id = ? and o_d_id = ? and o_w_id = ?";
		return sqlText;
	}
	
	/**
	 * UPDATE order_line SET ol_delivery_d = :datetime
	 * WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
	 * ol_w_id = :w_id;
	 */
	public static String deliveryStmt5(Dbms dbms) {
		String sqlText = "update order_line set ol_delivery_d = ? where ol_o_id = ?  and ol_d_id = ? and ol_w_id = ?";
		return sqlText;
	}
	
	/**
	 * SELECT SUM(ol_amount) INTO :ol_total
	 * FROM order_line
	 * WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
	 * AND ol_w_id = :w_id;
	 */
	public static String deliveryStmt6(Dbms dbms) {
		String sqlText = "select sum(ol_amount) as ol_total from order_line where ol_o_id = ? and ol_d_id = ? and ol_w_id = ?";
		return sqlText;
	}
	
	/**
	 * UPDATE customer SET c_balance = c_balance + :ol_total
	 * WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
	 */
	
	public static String deliveryStmt7(Dbms dbms) {
		String sqlText = "update customer set c_balance = c_balance + ? where c_id = ? and c_d_id = ? and c_w_id = ? ";
		return sqlText;
	}
	
	////// Stock-Level
	
	/**
	 * SELECT d_next_o_id INTO :o_id
	 * FROM district
	 * WHERE d_w_id=:w_id AND d_id=:d_id; 
	 */
	public static String stockLevelStmt1(Dbms dbms) {
		String sqlText = "select d_next_o_id from district where d_w_id = ? and d_id = ? ";
		return sqlText;
	}
	
	/**
	 * SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count 
     *         FROM order_line, stock
     *         WHERE ol_w_id=:w_id AND
     *               ol_d_id=:d_id AND ol_o_id<:o_id AND
     *               ol_o_id>=:o_id-20 AND s_w_id=:w_id AND
     *               s_i_id=ol_i_id AND s_quantity < :threshold;
	 */
	public static String stockLevelStmt2(Dbms dbms) {
		String sqlText = "select count( distinct(s_i_id)) as stock_count "
				+ "from order_line, stock "
				+ "where ol_w_id = ? and ol_d_id = ? and ol_o_id < ? "
				+ "and ol_o_id >= (? - 20) and s_w_id = ? and s_i_id = ol_i_id and s_quantity < ?";
		return sqlText;
	}
	
}
