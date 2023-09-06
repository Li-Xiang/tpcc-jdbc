package org.littlestar.tpcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.littlestar.tpcc.datasource.TpccDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TpccLoader implements TpccConstants {
	private final static Logger LOGGER = LoggerFactory.getLogger(TpccLoader.class);
	private final TpccDataSource dataSource ;
	private final Dbms dbms;
	private final int threads;
	private final int wareCount;
	
	public TpccLoader(TpccDataSource ds, Dbms dbms, int wareCount, int threads) {
		dataSource = ds;
		this.dbms = dbms;
		this.wareCount = wareCount > 0 ? wareCount : 1;
		this.threads = threads > 0 ? threads : 1;
	}
	
	public void createTpccTables() {
		LocalDateTime beginTime = LocalDateTime.now();
		LOGGER.info(">>>> Creating TPC-C's tables:");
		try (Connection connection = dataSource.getConnection()) {
			TpccLoad.createItem(connection, dbms);
			LOGGER.info("....   item done.");
			
			TpccLoad.createWarehouse(connection, dbms);
			LOGGER.info("....   warehouse done.");

			TpccLoad.createDistrict(connection, dbms);
			LOGGER.info("....   district done.");

			TpccLoad.createStock(connection, dbms);
			LOGGER.info("....   stock done.");

			TpccLoad.createCustomer(connection, dbms);
			LOGGER.info("....   customer done.");

			TpccLoad.createHistory(connection, dbms);
			LOGGER.info("....   history done.");

			TpccLoad.createOrders(connection, dbms);
			LOGGER.info("....   orders done.");

			TpccLoad.createNewOrders(connection, dbms);
			LOGGER.info("....   new_order done.");

			TpccLoad.createOrderLine(connection, dbms);
			LOGGER.info("....   order_line done.");
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			LOGGER.info(">>>> Tables created, elapsed " + runTime + " ms.");
		} catch (Exception e) {
			LOGGER.error("Create TPC-C tables failed.", e);
		}
	}
	
	public void doLoad() {
		LOGGER.info(">>>> TPC-C Data Loading...");
		LocalDateTime loadBeginTime = LocalDateTime.now();  
		int queueSize = 1 + 1 + wareCount + wareCount + wareCount * 10 + wareCount * 10;
		ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("tpcc-loader-pool-%d").build();
		ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(queueSize), factory, new ThreadPoolExecutor.AbortPolicy());
		LinkedList<Future<Long>> futures = new LinkedList<Future<Long>>();
		//// item
		Future<Long> itemFuture = executor.submit(new ItemLoader());
		futures.add(itemFuture);
		//// warehouse
		Future<Long> warehousesFuture = executor.submit(new WarehouseLoader());
		futures.add(warehousesFuture);
		
		for (int w = 1; w <= wareCount; w++) {
			int w_id = w;
			// stock threads (wareCount)
			Future<Long> stockFuture = executor.submit(new StockLoader(w_id));
			futures.add(stockFuture);
			
			// district threads (wareCount)
			Future<Long> districtFuture = executor.submit(new DistrictLoader(w_id));
			futures.add(districtFuture);
			for (int d = 1; d <= DIST_PER_WARE; d++) { // 每个仓库下的每个销售区域的销售数据;
				int d_id = d;
				///// customer & history threads (wareCount*10)
				Future<Long> customerFuture = executor.submit(new CustomerLoader(w_id, d_id));
				futures.add(customerFuture);
				
				///// orders & new-order & order-line threads (wareCount*10).
				Future<Long> ordersFuture = executor.submit(new OrdersLoader(w_id, d_id));
				futures.add(ordersFuture);
			}
		}
		executor.shutdown();
		
		long waitCount = 1;
		double rows = 0d;
		while (true) {
			int threadCount = futures.size();
			int doneCount = 0;
			for (Future<?> f : futures) {
				if (f.isDone()) {
					doneCount++;
				}
			}
			
			if (waitCount % 6000L == 0) {
				LOGGER.info(doneCount + " of " + threadCount + " tasks done.");
			}

			if (doneCount == threadCount) {
				for (Future<Long> f : futures) {
					try {
						rows += f.get();
					} catch (Exception e) {
					}
				}
				break;
			}
			try {
				TimeUnit.MICROSECONDS.sleep(100);
			} catch (InterruptedException e) {
			}
			waitCount++;
		}
		LocalDateTime loadEndTime = LocalDateTime.now();
		Duration duration = Duration.between(loadBeginTime, loadEndTime);
		long loadRunTime = duration.getSeconds();
		Double rps = (loadRunTime > 0) ? (rows/ loadRunTime) : 0l;
		LOGGER.info(">>>> TPC-C Data Load completed, elapsed " + loadRunTime + " secs. ( " + rps.longValue()+ " rows/sec ).");
		
	}

	public void doAddFksAndIndexes(boolean enableFk) {
		LOGGER.info(">>>> Creating TPC-C table's indexes and constraints:");
		try (Connection connection = dataSource.getConnection()) {
			addIndexes(connection);
			if(enableFk) {
				addForeignKeys(connection);
			}
			LOGGER.info(">>>> indexes and constraints done.");
		} catch (Exception e) {
			LOGGER.error("Create table's indexes and constraints failed.", e);
		}
	}
	
	public void doAddForeignKeys() {
		LOGGER.info(">>>> Creating TPC-C table's foreign keys:");
		try (Connection connection = dataSource.getConnection()) {
			addForeignKeys(connection);
			LOGGER.info(">>>> Foreign keys created.");
		} catch (Exception e) {
			LOGGER.error("Drop TPC-C table's foreign key failed.", e);
		}
	}
	
	private void addIndexes(Connection connection) throws Exception {
		String index1 = "create index idx_customer on customer (c_w_id,c_d_id,c_last,c_first)";
		executeUpdate(connection, index1);

		String index2 = "create index idx_orders on orders (o_w_id,o_d_id,o_c_id,o_id)";
		executeUpdate(connection, index2);

		String index3 = "create index fkey_stock_2 on stock (s_i_id)";
		executeUpdate(connection, index3);

		String index4 = "create index fkey_order_line_2 on order_line (ol_supply_w_id,ol_i_id)";
		executeUpdate(connection, index4);

		if (Objects.equals(dbms, Dbms.MSSQL)) {
			String index5 = "create index idx_stock_1 on stock (s_i_id, s_w_id)";
			executeUpdate(connection, index5);

			String index6 = "create index idx_district_1 on district (d_id, d_w_id)";
			executeUpdate(connection, index6);
		}

		if (Objects.equals(dbms, Dbms.SQLite)) {
			// SQLite does not support ADD CONSTRAINT in the ALTER TABLE statement.
			String uk1 = "create unique index if not exists warehouse_pk on warehouse(w_id)";
			executeUpdate(connection, uk1);
			String uk2 = "create unique index if not exists district_pk on district(d_w_id, d_id)";
			executeUpdate(connection, uk2);
			String uk3 = "create unique index if not exists customer_pk on customer(c_w_id, c_d_id, c_id)";
			executeUpdate(connection, uk3);
			String uk4 = "create unique index if not exists new_order_pk on new_order(no_w_id, no_d_id, no_o_id)";
			executeUpdate(connection, uk4);
			String uk5 = "create unique index if not exists orders_pk on orders(o_w_id, o_d_id, o_id)";
			executeUpdate(connection, uk5);
			String uk6 = "create unique index if not exists order_line_pk on order_line(ol_w_id, ol_d_id, ol_o_id, ol_number)";
			executeUpdate(connection, uk6);
			String uk7 = "create unique index if not exists item_pk on item(i_id)";
			executeUpdate(connection, uk7);
			String uk8 = "create unique index if not exists stock_pk on stock(s_w_id, s_i_id)";
			executeUpdate(connection, uk8);
		} else {
			String pk1 = "alter table warehouse add constraint warehouse_pk primary key (w_id)";
			executeUpdate(connection, pk1);
			String pk2 = "alter table district add constraint district_pk primary key (d_w_id, d_id)";
			executeUpdate(connection, pk2);
			String pk3 = "alter table customer add constraint customer_pk primary key (c_w_id, c_d_id, c_id)";
			executeUpdate(connection, pk3);
			String pk4 = "alter table new_order add constraint new_order_pk primary key (no_w_id, no_d_id, no_o_id)";
			executeUpdate(connection, pk4);
			String pk5 = "alter table orders add constraint orders_pk primary key (o_w_id, o_d_id, o_id)";
			executeUpdate(connection, pk5);
			String pk6 = "alter table order_line add constraint order_line_pk primary key(ol_w_id, ol_d_id, ol_o_id, ol_number)";
			executeUpdate(connection, pk6);
			String pk7 = "alter table item add constraint item_pk primary key (i_id)";
			executeUpdate(connection, pk7);
			String pk8 = "alter table stock add constraint stock_pk primary key (s_w_id, s_i_id)";
			executeUpdate(connection, pk8);
		}
	}
	
	private void addForeignKeys(Connection connection) throws Exception {
		if(Objects.equals(dbms, Dbms.SQLite)) {
			// SQLite does not support ADD CONSTRAINT in the ALTER TABLE statement.
			return;
		}
		String fk1 = "alter table district add constraint fkey_district_1 foreign key(d_w_id) references warehouse(w_id)";
		executeUpdate(connection, fk1);
		String fk2 = "alter table customer add constraint fkey_customer_1 foreign key(c_w_id,c_d_id) references district(d_w_id,d_id)";
		executeUpdate(connection, fk2);
		String fk3 = "alter table history add constraint fkey_history_1 foreign key(h_c_w_id,h_c_d_id,h_c_id) references customer(c_w_id,c_d_id,c_id)";
		executeUpdate(connection, fk3);
		String fk4 = "alter table history add constraint fkey_history_2 foreign key(h_w_id,h_d_id) references district(d_w_id,d_id)";
		executeUpdate(connection, fk4);
		String fk5 = "alter table new_order add constraint fkey_new_orders_1 foreign key(no_w_id,no_d_id,no_o_id) references orders(o_w_id,o_d_id,o_id)";
		executeUpdate(connection, fk5);
		String fk6 = "alter table orders add constraint fkey_orders_1 foreign key(o_w_id,o_d_id,o_c_id) references customer(c_w_id,c_d_id,c_id)";
		executeUpdate(connection, fk6);
		String fk7 = "alter table order_line add constraint fkey_order_line_1 foreign key(ol_w_id,ol_d_id,ol_o_id) references orders(o_w_id,o_d_id,o_id)";
		executeUpdate(connection, fk7);
		String fk8 = "alter table order_line add constraint fkey_order_line_2 foreign key(ol_supply_w_id,ol_i_id) references stock(s_w_id,s_i_id)";
		executeUpdate(connection, fk8);
		String fk9 = "alter table stock add constraint fkey_stock_1 foreign key(s_w_id) references warehouse(w_id)";
		executeUpdate(connection, fk9);
		String fk10 = "alter table stock add constraint fkey_stock_2 foreign key(s_i_id) references item(i_id)";
		executeUpdate(connection, fk10);
	}
	
	public void doDropForeignKeys() {
		LOGGER.info(">>>> droping TPC-C table's foreign-keys:");
		try (Connection connection = dataSource.getConnection()) {
			dropForeignKeys(connection);
			LOGGER.info(">>>> Foreign keys droped.");
		} catch (Exception e) {
			LOGGER.error("Drop TPC-C tables foreign key failed.", e);
		}
	}
	
	private void dropForeignKeys(Connection connection) throws Exception {
		
		String fk1 = "alter table district drop constraint fkey_district_1";
		String fk2 = "alter table customer drop constraint fkey_customer_1";
		String fk3 = "alter table history drop constraint fkey_history_1";
		String fk4 = "alter table history drop constraint fkey_history_2";
		String fk5 = "alter table new_order drop constraint fkey_new_orders_1";
		String fk6 = "alter table orders drop constraint fkey_orders_1";
		String fk7 = "alter table order_line drop constraint fkey_order_line_1";
		String fk8 = "alter table order_line drop constraint fkey_order_line_2";
		String fk9 = "alter table stock drop constraint fkey_stock_1";
		String fk10 = "alter table stock drop constraint fkey_stock_2";
		
		if(Objects.equals(dbms, Dbms.MySQL)) {
			fk1 = "alter table district drop foreign key fkey_district_1";
			fk2 = "alter table customer drop foreign key fkey_customer_1";
			fk3 = "alter table history drop foreign key fkey_history_1";
			fk4 = "alter table history drop foreign key fkey_history_2";
			fk5 = "alter table new_order drop foreign key fkey_new_orders_1";
			fk6 = "alter table orders drop foreign key fkey_orders_1";
			fk7 = "alter table order_line drop foreign key fkey_order_line_1";
			fk8 = "alter table order_line drop foreign key fkey_order_line_2";
			fk9 = "alter table stock drop foreign key fkey_stock_1";
			fk10 = "alter table stock drop foreign key fkey_stock_2";
		}
		
		executeUpdate(connection, fk1);
		executeUpdate(connection, fk2);
		executeUpdate(connection, fk3);
		executeUpdate(connection, fk4);
		executeUpdate(connection, fk5);
		executeUpdate(connection, fk6);
		executeUpdate(connection, fk7);
		executeUpdate(connection, fk8);
		executeUpdate(connection, fk9);
		executeUpdate(connection, fk10);
	}
	
	
	private void dropTables(Connection connection) {
		String tab1 = "drop table item";
		executeUpdate(connection, tab1);
		String tab2 = "drop table district";
		executeUpdate(connection, tab2);
		String tab3 = "drop table customer";
		executeUpdate(connection, tab3);
		String tab4 = "drop table history";
		executeUpdate(connection, tab4);
		String tab5 = "drop table new_order";
		executeUpdate(connection, tab5);
		String tab6 = "drop table orders";
		executeUpdate(connection, tab6);
		String tab7 = "drop table order_line";
		executeUpdate(connection, tab7);
		String tab8 = "drop table stock";
		executeUpdate(connection, tab8);
		String tab9 = "drop table warehouse";
		executeUpdate(connection, tab9);
	}
	
	
	public void doDropTables() {
		LOGGER.info(">>>> Drop TPC-C tables:");
		try (Connection connection = dataSource.getConnection()) {
			dropForeignKeys(connection);
			dropTables(connection);
			LOGGER.info(">>>> Tables droped.");
		} catch (Exception e) {
			LOGGER.error("Drop TPC-C tables failed.", e);
		}
	}
	
	class WarehouseLoader implements Callable<Long> {
		@Override
		public Long call() throws Exception {
			long rows = 0L;
			LocalDateTime beginTime = LocalDateTime.now();
			LOGGER.info("Loading Warehouse data...");
			try (Connection connection = dataSource.getConnection()){
				rows = TpccLoad.loadWarehouse(connection, dbms, wareCount);
			} catch (Exception e) {
				LOGGER.error("Load Warehouses data failed, abort.", e);
				System.exit(202);
			}
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			long rps = (runTime > 0) ? (rows * 1000l) / runTime : 0l;
			LOGGER.info("Warehouse done, " + rows + " rows, elapsed " + runTime + " ms. (" + rps + " rows/sec)");
			return rows;
		}
	}
	
	class ItemLoader implements Callable<Long> {
		@Override
		public Long call() throws Exception {
			long rows = 0l;
			LocalDateTime beginTime = LocalDateTime.now();
			LOGGER.info("Loading Item data... ");
			try(Connection connection = dataSource.getConnection()) {
				rows = TpccLoad.loadItem(connection, dbms);
			} catch (Exception e) {
				LOGGER.error("Load Item failed, abort.", e);
				System.exit(201);
			}
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			long rps = (runTime > 0) ? (rows * 1000l) / runTime : 0l;
			LOGGER.info("Item done, " + rows + " rows, elapsed " + runTime + " ms. (" + rps + " rows/sec)");
			return rows;
		}
	};

	class StockLoader implements Callable<Long> {
		int w_id;

		public StockLoader(int wid) {
			this.w_id = wid;
		}

		@Override
		public Long call() throws Exception {
			long rows = 0L;
			LocalDateTime beginTime = LocalDateTime.now();
			LOGGER.info("Loading Stock (w_id=" + w_id + " of " + wareCount + ") ...");
			try(Connection connection = dataSource.getConnection()) {
				rows = TpccLoad.loadStock(connection, dbms, w_id);
			} catch (Exception e) {
				LOGGER.error("Load Stock (w_id=" + w_id + ") failed, abort.", e);
				System.exit(203);
			}
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			long rps = (runTime > 0) ? (rows * 1000L) / runTime : 0L;
			LOGGER.info("Stock (" + w_id + " of " + wareCount + ") done, " + rows + " rows, elapsed " + runTime + " ms. (" + rps + " rows/sec)");
			return rows;
		}
	}

	class DistrictLoader implements Callable<Long> {
		int w_id;

		public DistrictLoader(int w_id) {
			this.w_id = w_id;
		}

		@Override
		public Long call() throws Exception {
			long rows = 0l;
			LocalDateTime beginTime = LocalDateTime.now();
			LOGGER.info("Loading District Wid=" + w_id + " ... ");
			try(Connection connection = dataSource.getConnection()) {
				rows = TpccLoad.loadDistrict(connection, dbms, w_id);
			} catch (Exception e) {
				LOGGER.error("Loading District Wid=" + w_id + " failed, abort.", e);
				System.exit(204);
			}
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			long rps = (runTime > 0) ? (rows * 1000l) / runTime : 0l;
			LOGGER.info("District Wid=" + w_id + " done, " + rows + " rows, elapsed " + runTime + " ms. (" + rps + " rows/sec)");
			return rows;
		}
	}
	
	class CustomerLoader implements Callable<Long> {
		int w_id;
		int d_id;

		public CustomerLoader(int w_id, int d_id) {
			this.w_id = w_id;
			this.d_id = d_id;
		}

		@Override
		public Long call() throws Exception {
			long rows = 0L;
			LocalDateTime beginTime = LocalDateTime.now();
			LOGGER.info("Loading Customer, History for Did=" + d_id + ", Wid=" + w_id + " ... ");
			try (Connection connection = dataSource.getConnection()) {
				rows = TpccLoad.loadCustomer(connection, dbms, d_id, w_id);
			} catch (Exception e) {
				LOGGER.error("Loading Customer, History for Did=" + d_id + ", Wid=" + w_id + " failed, abort.", e);
				System.exit(205);
			}
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			long rps = (runTime > 0) ? (rows * 1000l) / runTime : 0l;
			LOGGER.info("Customer, History for Did=" + d_id + ", Wid=" + w_id + " done, " + rows + " rows, elapsed "
					+ runTime + " ms. (" + rps + " rows/sec)");
			return rows;
		}
	}

	///// Orders & New-Orders & Order-Line Loader threads.
	class OrdersLoader implements Callable<Long> {
		int d_id;
		int w_id;

		public OrdersLoader(int w_id, int d_id) {
			this.d_id = d_id;
			this.w_id = w_id;
		}

		@Override
		public Long call() throws Exception {
			long rows = 0l;
			LocalDateTime beginTime = LocalDateTime.now();
			LOGGER.info("Loading Orders, New-Order, Order-Line for Did=" + d_id + ", Wid=" + w_id + " ... ");
			try (Connection connection = dataSource.getConnection()) {
				rows = TpccLoad.loadOrders(connection, dbms, d_id, w_id);
			} catch (Exception e) {
				LOGGER.error("Loading Orders, New-Order, Order-Line for Did=" + d_id + ", Wid=" + w_id + " failed, abort.",e);
				System.exit(206);
			}
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			long rps = (runTime > 0) ? (rows * 1000l) / runTime : 0l;
			LOGGER.info("Orders, New-Order, Order-Line for Did=" + d_id + ", Wid=" + w_id + " done, " + rows
					+ " rows, elapsed " + runTime + " ms. (" + rps + " rows/sec)");
			return rows;
		}
	}

	public void checkTableRows() throws Exception {
		LOGGER.info(">>>> Checking TPC-C Tables:");
		LOGGER.info("...... Warehouse (w)                     : " + warehouseRows() + " rows.");
		LOGGER.info("...... Item (100000)                     : " + itemRows() + " rows.");
		LOGGER.info("...... Stock (w*100000)                  : " + stockRows() + " rows.");
		LOGGER.info("...... District (w*10)                   : " + districtRows() + " rows.");
		LOGGER.info("...... Customer (w*10*3000)              : " + customerRows() + " rows.");
		LOGGER.info("...... Order (number of customers)       : " + ordersRows() + " rows.");
		LOGGER.info("...... New-Order (30% of the orders)     : " + newOrderRows() + " rows.");
		LOGGER.info("...... Order-Line (approx. 10 per order) : " + orderLineRows() + " rows.");
		LOGGER.info("...... History (number of customers)     : " + historyRows()+ " rows.");
		LOGGER.info(">>>> done.");
	}
	
	public long warehouseRows() throws Exception {
		int rows = -1;
		String sqlText = "select count(*) as cnt from warehouse";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if (rs.next()) {
				rows = rs.getInt(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long itemRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from item";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if(rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long stockRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from stock";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if (rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long customerRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from customer";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if (rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long districtRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from district";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if(rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long newOrderRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from new_order";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if (rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long orderLineRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from order_line";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if(rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long ordersRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from orders";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if (rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}
	
	public long historyRows() throws Exception {
		long rows = -1;
		String sqlText = "select count(*) as cnt from history";
		try (Connection connection = dataSource.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sqlText);
			if (rs.next()) {
				rows = rs.getLong(1);
			}
		} catch (Exception e) {
			throw e;
		}
		return rows;
	}

	private static long executeUpdate(Connection connection, String sqlText) {
		LocalDateTime beginTime = LocalDateTime.now();
		try {
			Statement stmt = connection.createStatement();
			stmt.executeUpdate(sqlText);
			stmt.close();
			LocalDateTime endTime = LocalDateTime.now();
			Duration duration = Duration.between(beginTime, endTime);
			long runTime = duration.toMillis();
			LOGGER.info(".... " + sqlText +", elapsed: " + runTime + " ms.");
			return runTime;
		} catch (Exception e) {
			LOGGER.error("execute update failed.", e);
		}
		return -1;
	}
}
