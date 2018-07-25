#include "Database.h"



Database *Database::instance = 0;

Database::Database() : driver(get_driver_instance()) {
	connection_properties["hostName"] = DB_HOST;
	connection_properties["port"] = DB_PORT;
	connection_properties["userName"] = DB_USER;
	connection_properties["password"] = DB_PASS;
	connection_properties["OPT_RECONNECT"] = true;
	
	// use database
	try {
		Connection *con = driver->connect(connection_properties);
		try {
			con->setSchema(DB_NAME);
		}
		catch (SQLException &e) {
			cout << "Loading Data Base. This May Take a While...." << endl;
			Statement *stmt = con->createStatement();
			string q = "CREATE DATABASE IF NOT EXISTS ";
			q.append(DB_NAME);
			stmt->execute(q);
			con->setSchema(DB_NAME); // switch database
			stmt->execute("CREATE TABLE IF NOT EXISTS supplier( "
				"supplier_num INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
				"supplier_name VARCHAR(50)"
				")");

			stmt->execute("CREATE TABLE IF NOT EXISTS client("
				"client_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
				"first_name VARCHAR(20), "
				"last_name VARCHAR(20), "
				"phone_num VARCHAR(50), "
				"join_date DATE, "
				"sum_this_year INT UNSIGNED , "
				"total_sum INT UNSIGNED "
				")");

			stmt->execute("CREATE TABLE IF NOT EXISTS orders(order_num INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, order_date DATE,client_id INT UNSIGNED,"
				"supplier_num INT UNSIGNED, order_status VARCHAR(50), FOREIGN KEY(client_id) references client(client_id))");

			stmt->execute("CREATE TABLE IF NOT EXISTS deal("
				"deal_num INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
				"deal_val INT UNSIGNED, "
				"client_id INT UNSIGNED, "
				"discount FLOAT UNSIGNED, "
				"emp_id INT UNSIGNED, "
				"is_canceled BIT, "
				"deal_date DATE, "
				"FOREIGN KEY(client_id) references client(client_id) "
				"ON DELETE RESTRICT "
				")");

			stmt->execute("CREATE TABLE IF NOT EXISTS book("
				"name VARCHAR(50) PRIMARY KEY, "
				"Author VARCHAR(30) NOT NULL, "
				"price INT UNSIGNED, "
				"supplier_price INT UNSIGNED,"
				"max_stock INT UNSIGNED, "
				"current_stock INT UNSIGNED, "
				"global_discount FLOAT UNSIGNED, "
				"CHECK (max_stock > current_stock)"
				")");

			stmt->execute("CREATE TABLE IF NOT EXISTS worker("
				"id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "
				"first_name VARCHAR(30) NOT NULL, "
				"last_name VARCHAR(30) NOT NULL"
				")");


			stmt->execute("CREATE TABLE IF NOT EXISTS supplier_book("
				"supplier_num INT UNSIGNED, "
				"book_name VARCHAR(50) NOT NULL, "
				"PRIMARY KEY(supplier_num, book_name) "
				")");

			stmt->execute("CREATE TABLE IF NOT EXISTS deal_book("
				"deal_num INT UNSIGNED, "
				"book_name VARCHAR(50) NOT NULL, "
				"PRIMARY KEY(deal_num, book_name) "
				")");

			stmt->execute("CREATE TABLE IF NOT EXISTS order_book("
				"order_num INT UNSIGNED, "
				"book_name VARCHAR(50) NOT NULL, "
				"PRIMARY KEY(order_num, book_name) "
				")");

			
			
			addBooks();
			addClients();
			addSuppliers();
			addWorkers();
			addDeals();
			addOrders();

			//stmt->execute("DROP DATABASE book_store;");

			delete stmt;

		}

		delete con;
		cout << "Data Base Loaded!" << endl;
	}
	catch (SQLException &e) {
		cout << e.getErrorCode() << " " << e.what() << " " << e.getSQLState();
	}
}

Database & Database::getInstance() {
	if (Database::instance == 0) {
		instance = new Database();
	}
	return *instance;
}

Connection * Database::getConnection() {
	try {
		Connection *con = driver->connect(connection_properties);
		con->setSchema(DB_NAME);
		return con;
	} catch (SQLException &e) {
		cout << e.what();
	}
	return 0;
}

void Database::addBooks(){
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	stmt->execute("insert into book(name, author, price, supplier_price, max_stock, current_stock, global_discount)"
													 "values('The Book Of Mormons', 'James Dean', 150, 35, 350, 299, 0.1)," 
													 "('Harry Potter And The Philosophers Stone', 'J.K Rolling', 200, 30,150, 98, 0),"
													 "('Harry Potter And The Chamber of Secrets', 'J.K Rolling', 150, 20 ,260, 163, 0.1),"
													 "('Harry Potter And The Prisoner of Azkaban', 'J.K Rolling', 150, 50, 450, 35, 0),"
													 "('A Song Of Ice And Fire 1: A Game Of Thrones', 'George R.R Martin',250, 35, 350, 71, 0),"
													 "('A Song Of Ice And Fire 2: A Clash Of Kings', 'George R.R Martin', 300, 45, 150, 86, 0),"
													 "('A Song Of Ice And Fire 3: A Storm Of Swords', 'George R.R Martin', 250, 45, 500, 143, 0),"
													 "('A Song Of Ice And Fire 4: A Feast For Crows', 'George R.R Martin', 150, 10,175, 68, 0.1),"
													 "('Enders Game', 'Orson Scott Card', 135, 15,350, 175, 0),"
													 "('Enders Shadow', 'Orson Scott Card', 250, 30,550, 196, 0.1),"
													 "('It, First Edition', 'Stephen King', 750, 300,350, 0, 0);");
	
	delete con;
	delete stmt;
}

void Database::addWorkers() {
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	stmt->execute("insert into worker(first_name, last_name) values('Elmira', 'Luter'),('Anya', 'Higgan'),('Nancie', 'Cutten'),"
													 "('Estrella', 'Rehor'),('Cherise', 'Kingswoode');");

	delete con;
	delete stmt;

}


void Database::addClients(){
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	stmt->execute("insert into client (first_name, last_name, phone_num, join_date, sum_this_year, total_sum) values ('Aggi', 'Blanpein', '804 486 8130', '1998-12-02', 0, 0),"
													 "('Silvester', 'Hainn', '509 890 7069', '1997-09-08', 0, 0),('Pepillo', 'Stolle', '505 193 6383', '2000-03-06', 0, 0),"
													 "('Whit', 'Kevane', '215 246 6187', '1998-08-02', 0, 0),('Humphrey', 'Kinglake', '955 602 2484', '2005-10-06', 0, 0),"
													 "('Frasquito', 'Beamish', '236 569 0397', '2007-07-17', 0, 0),('Teresina', 'Yepisko', '155 501 9829', '2002-10-02', 0, 0),"
													 "('Grace', 'Airton', '128 818 9009', '2009-11-11', 0, 0),('Quill', 'Glamart', '557 667 4766', '1999-09-06', 0, 0),('Elsey', 'Edge', '823 661 2828', '2003-03-24', 0, 0);");
	
	delete con;
	delete stmt;
}

void Database::addSuppliers(){
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	stmt->execute("insert into supplier (supplier_name) values ('Schmidt Group'),('McClure-Bartoletti'),"
													 "('Smitham-Cormier'),('Goyette-Barrows'),('Hagenes and Sons'),('Pollich and Sons'),('Grimes-Denesik'),"
													 "('Kunze and Sons'),('Powlowski-Tremblay'),('Franecki, Rath and Osinski'),('Goodwin, Douglas and Cole'),"
													 "('Kuphal and Sons');");

	stmt->execute("insert into supplier_book (supplier_num, book_name) values (2, 'Enders Game'), (2,'Harry Potter And The Chamber Of Secrets'), (2, 'Enders Shadow'), (2, 'It, First Edition'), (1, 'A Song Of Ice And Fire 4: A Feast For Crows'), (1, 'The Book Of Mormons'), "
				  "(3, 'Harry Potter And The Philosophers Stone'), (3, 'Enders Game'), (3, 'Enders Shadow'), (5, 'Harry Potter And The Philosophers Stone'), "
				  "(7, 'Harry Potter And The Chamber Of Secrets'), (8, 'A Song Of Ice And Fire 3: A Storm Of Swords'), (8, 'It, First Edition'), (11, 'A Song Of Ice And Fire 2: A Clash Of Kings'), "
				  "(12, 'Harry Potter And The Prisoner Of Azkaban'), (7, 'A Song Of Ice And Fire 4: A Feast For Crows'), (12, 'Harry Potter And The Philosophers Stone'), (3, 'A Song Of Ice And Fire 1: A Game Of Thrones'), (6, 'A Song Of Ice And Fire 4: A Feast For Crows'), (7, 'It, First Edition'), (1, 'Harry Potter And The Philosophers Stone'), (1, 'Enders Shadow'), (10, 'It, First Edition');");

	delete con;
	delete stmt;

}

void Database::addDeals(){
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	stmt->execute("insert into deal_book (deal_num, book_name) values(5, 'Harry Potter And The Philosophers Stone'),(5, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(9, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(9, 'The Book Of Mormons'),(9, 'Enders Shadow'),(8, 'The Book Of Mormons'),"
		"(5, 'Harry Potter And The Chamber Of Secrets'),(6, 'A Song Of Ice And Fire 1: A Game Of Thrones'),(8, 'Enders Game'),(5, 'Enders Game'),(11, 'Harry Potter And The Chamber Of Secrets'),"
		"(8, 'A Song Of Ice And Fire 1: A Game Of Thrones'),(8, 'Harry Potter And The Prisoner Of Azkaban'),(1, 'It, First Edition'),(1, 'Enders Shadow'),(2, 'Enders Shadow'),(3, 'A Song Of Ice And Fire 2: A Clash Of Kings'),"
		"(4, 'Enders Game'),(7, 'Harry Potter And The Prisoner Of Azkaban'),(2, 'A Song Of Ice And Fire 1: A Game Of Thrones'),(10, 'A Song Of Ice And Fire 2: A Clash Of Kings'),"
		"(12, 'It, First Edition'),(12, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(13, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(13, 'Enders Shadow'),"
		"(13, 'It, First Edition'),(14, 'The Book Of Mormons'),(14, 'Harry Potter And The Chamber Of Secrets'),(14, 'Enders Shadow'),(14, 'A Song Of Ice And Fire 2: A Clash Of Kings');");


	stmt->execute("insert into deal (deal_val, client_id, discount, emp_id, is_canceled, deal_date) values(0, 1, 0.2, 3, false, '1998-12-02'),(0, 2, 0.1, 2, false, '1997-09-08'),(0, 3, 0.4, 1, true, '2000-03-06'),"
								  "(0, 4, 0.3, 4, false, '1998-08-02'),(0, 4, 0.2, 1, false, '2016-03-26'),(0, 5, 0.3, 3, false, '2005-10-06'),(0, 6, 0.3, 4, false, '2018-07-17'),(0, 10, 0.1, 5, false, '2003-03-24'),"
								  "(0, 6, 0.3, 3, false, '2015-06-06'),(0, 7, 0.3, 4, false, '2017-11-11'),(0, 8, 0.2, 2, false, '2002-10-02'),(0, 8, 0.2, 3, false, '2018-03-06'),(0, 9, 0.1, 1, false, '2017-09-06'),(0, 10, 0.2, 1, true, '2017-10-29');");


	stmt->execute("CREATE TABLE  temp AS SELECT deal_num, SUM(price) as val FROM deal_book inner join book where book.name = deal_book.book_name group by deal_num;");

	stmt->execute("UPDATE deal, temp SET deal.deal_val = temp.val WHERE deal.deal_num = temp.deal_num;");
	stmt->execute("DROP TABLE temp;");

	/* Updating total sum + sum this year, of deals in clients*/
	stmt->execute("CREATE TABLE temp as SELECT deal.client_id, SUM(CEILING(deal_val * (1 - deal.discount))) AS val from deal WHERE is_canceled = 0 group by deal.client_id;");
	stmt->execute("UPDATE client, temp SET total_sum = temp.val WHERE client.client_id = temp.client_id;");
	stmt->execute("DROP TABLE temp;");

	stmt->execute("CREATE TABLE temp as SELECT deal.client_id, SUM(CEILING(deal_val * (1 - deal.discount))) as val FROM deal WHERE is_canceled = 0 AND deal.deal_date BETWEEN(curdate() - INTERVAL 1 YEAR) AND curdate() group by deal.client_id;");
	stmt->execute("UPDATE client, temp SET sum_this_year = temp.val WHERE client.client_id = temp.client_id;");
	stmt->execute("DROP TABLE temp;");
	delete con;
	delete stmt;
	
}

void Database::addOrders(){
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	stmt->execute("INSERT INTO orders (order_date, client_id, supplier_num, order_status) values('2013/04/08', 10, 2, 'Closed'),('1998/12/01', 1, 2, 'Closed'),('2009/11/25', 5, 1, 'Closed'),('2017/12/27', 9, 3, 'Ordered'),('2017/08/31', 10, 5, 'Arrived'),('2018/02/11', 9, 3, 'Sent Message'),('2017/10/12', 4, 8, 'Arrived'),('2018/01/15', 5, 12, 'Ordered'),('2011/07/11', 4, 11, 'Closed'),('2018/04/07', 7, 7, 'Closed');");

	stmt->execute("INSERT INTO order_book (order_num, book_name) values(10, 'Harry Potter And The Chamber Of Secrets'),(9, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(6, 'Enders Shadow'),(8, 'Harry Potter And The Prisoner Of Azkaban'),(7,'It, First Edition'),"
				  "(3, 'The Book Of Mormons'),(2, 'Enders Shadow'),(1, 'Harry Potter And The Chamber Of Secrets'),(5, 'Harry Potter And The Philosophers Stone'),(3, 'A Song Of Ice And Fire 4: A Feast For Crows'),(7, 'A Song Of Ice And Fire 3: A Storm Of Swords'),(6, 'Enders Game'),(4, 'Harry Potter And The Philosophers Stone'),(2, 'It, First Edition'),(1, 'Enders Game');");

	delete con;
	delete stmt;
}

void Database::allBooks(){
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	ResultSet *rset =  stmt->executeQuery("SELECT * FROM book WHERE current_stock != 0");

	rset->beforeFirst();
	cout << "Books Currently In Stock:" << endl;

	while (rset->next()) {
		cout << rset->getString("name") << ". By: " << rset->getString("Author") << "." << endl;
	}
}

void Database::openOrders() {

	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	ResultSet *rset = stmt->executeQuery("SELECT * FROM orders inner join order_book WHERE order_status != 'Closed' AND orders.order_num = order_book.order_num;");

	rset->beforeFirst();
	cout << "Open Orders:" << endl;

	while (rset->next()) {
		cout << "Order Number: " << rset->getUInt("order_num") << ". Order Date: " << rset->getString("order_date") << ". Status: " << rset->getString("order_status") << "." << endl;
	}
}

void Database::allClients() {
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	ResultSet *rset = stmt->executeQuery("SELECT * FROM client;");

	rset->beforeFirst();
	cout << "Clients:" << endl;

	while (rset->next()) {
		cout << "Client ID: " << rset->getUInt("client_id") << ".\tClient Name: " 
			 << rset->getString("first_name") << " " << rset->getString("last_name") << ".\t\tPhone Number: " 
			 << rset->getString("phone_num") << "." << endl;
	}
}