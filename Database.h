#ifndef _DB_H
#define _DB_H

#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

using namespace sql;
using namespace std;
#define DB_NAME "Book_Store"
#define DB_HOST "tcp://127.0.0.1/"
#define DB_PORT 3306
#define DB_USER "root"
#define DB_PASS "ghsigzri90"

class Database {
private:
	// singleton instance
	static Database *instance;

	Driver *driver;
	ConnectOptionsMap connection_properties;

	// private CTOR (singleton)
	Database();
public:
	static Database& getInstance();

	Connection *getConnection();

	virtual ~Database() {}

	void addBooks();
	void addClients();
	void addSuppliers();
	void addDeals();
	void addOrders();
	void addWorkers();
	
	/* Menu Options Functions */
	void allBooks();
	void openOrders();
	void allClients();
	void allSuppliers();
	void dealsInDates();
	void booksOnDiscount();
	void checkStock();
	void bookSupplier();
	void bookSoldSince();
	void booksClientBoughtSince();
	void clientsJoinedAfterDate();
	void topClient();
	void ordersSince();
	void workersSales();
	void topSupplierOrder();
	void totalDiscount();
	void top10Books();
	void supplierPurchases();
	void ordeToDeal();
	void quarterProfit();
};



#endif