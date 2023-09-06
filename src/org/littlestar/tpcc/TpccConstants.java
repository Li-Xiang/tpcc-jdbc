package org.littlestar.tpcc;

import java.io.File;

public interface TpccConstants {
	public static final String USER_DIR = System.getProperty("user.dir") + File.separator;
	//TPC-C规范约定:
	public static final int MAX_ITEMS     = 100000;  //每个仓库的商品条目数都是100000;
	public static final int DIST_PER_WARE = 10;      //每个仓库负责为10个销售区域供货;
	public static final int CUST_PER_DIST = 3000;    //每个销售区域有3000个客户;
	public static final int ORD_PER_DIST  = 3000;    //每个区域有3000个订单;
	public static final int MAX_NUM_ITEMS = 15;
	
	public static final int MAX_RETRY = 15;  //2000;
	public static final int SQL_BATCH_SIZE  = 1000;
	
}
