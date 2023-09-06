package org.littlestar.tpcc;

/**
	New-Order(no)       n/a  (0.45) #=0;
	Payment(py)	        43.0        #=1; 
	Order-Status(os)	4.0         #=2;
	Delivery(dl)	    4.0         #=3;
	Stock-Level(sl)	    4.0         #=4;
*/
public enum TransactionType {
	NewOrder(0), Payment(1), OrderStatus(2), Delivery(3), StockLevel(4);

	private final int id;

	TransactionType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}
}
