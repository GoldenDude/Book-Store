#include "Database.h"
#include <iostream>

void clearCin() {
	string temp;
	getline(cin, temp);
}


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
		"(5, 'Harry Potter And The Chamber Of Secrets'),(6, 'A Song Of Ice And Fire 4: A Feast For Crows'),(6, 'The Book Of Mormons'),(8, 'Enders Game'),(5, 'Enders Game'),(11, 'Harry Potter And The Chamber Of Secrets'),"
		"(8, 'A Song Of Ice And Fire 1: A Game Of Thrones'),(8, 'Harry Potter And The Prisoner Of Azkaban'),(1, 'It, First Edition'),(1, 'Enders Shadow'),(2, 'Enders Shadow'),(3, 'A Song Of Ice And Fire 2: A Clash Of Kings'),"
		"(4, 'Enders Game'),(7, 'Harry Potter And The Prisoner Of Azkaban'),(2, 'A Song Of Ice And Fire 1: A Game Of Thrones'),(10, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(10, 'Harry Potter And The Chamber Of Secrets'),(10, 'Enders Game'),"
		"(12, 'It, First Edition'),(12, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(13, 'A Song Of Ice And Fire 2: A Clash Of Kings'),(13, 'Enders Shadow'),"
		"(13, 'It, First Edition'),(14, 'The Book Of Mormons'),(14, 'Harry Potter And The Chamber Of Secrets'),(14, 'Enders Game');");


	stmt->execute("insert into deal (deal_val, client_id, discount, emp_id, is_canceled, deal_date) values(0, 1, 0.2, 3, false, '1998-12-02'),(0, 2, 0.1, 2, false, '1997-09-08'),(0, 3, 0.4, 1, true, '2000-03-06'),"
								  "(0, 4, 0.3, 4, false, '1998-08-02'),(0, 4, 0.2, 1, false, '2011-07-15'),(0, 5, 0.3, 3, false, '2009-11-27'),(0, 6, 0.3, 4, false, '2018-07-17'),(0, 10, 0.1, 5, false, '2003-03-24'),"
								  "(0, 6, 0.3, 3, false, '2015-06-06'),(0, 7, 0.3, 4, false, '2018-04-11'),(0, 8, 0.2, 2, false, '2002-10-02'),(0, 8, 0.2, 3, false, '2018-03-06'),(0, 9, 0.1, 1, false, '2017-09-06'),(0, 10, 0.2, 1, false, '2013-04-10');");


	stmt->execute("CREATE TABLE  temp AS SELECT deal_num, SUM(price) as val FROM deal_book inner join book where book.name = deal_book.book_name group by deal_num;");

	stmt->execute("UPDATE deal, temp SET deal.deal_val = temp.val WHERE deal.deal_num = temp.deal_num;");
	stmt->execute("DROP TABLE temp;");

	/* Updating total sum + sum this year, of deals in clients */
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
	int counter = 1;
	rset->beforeFirst();
	cout << "Books Currently In Stock:" << endl;

	while (rset->next()) {
		cout << counter << ") " << rset->getString("name") << ". By: " << rset->getString("Author") << "." << endl;
		++counter;
	}

	delete con;
	delete stmt;
	delete rset;
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

	delete con;
	delete stmt;
	delete rset;
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

	delete con;
	delete stmt;
	delete rset;
}

void Database::allSuppliers() {
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	ResultSet *rset = stmt->executeQuery("SELECT * FROM supplier;");

	rset->beforeFirst();
	cout << "Supplier:" << endl;

	while (rset->next()) {
		cout << "Supplier Number: " << rset->getUInt("supplier_num") << ".\tSupplier Name: "
			 << rset->getString("supplier_name") << "." << endl;
	}

	delete con;
	delete stmt;
	delete rset;
}

void Database::dealsInDates() {
	string date;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;	
	PreparedStatement *pstmt = con->prepareStatement("SELECT * FROM deal WHERE deal.deal_date BETWEEN ? AND ?;");

	cout << "Please Enter Start Date:> ";
	cin >> date;
	pstmt->setString(1, date);

	cout << "Please Enter End Date:> ";
	cin >> date;
	
	pstmt->setString(2, date);
	
	rset = pstmt->executeQuery();

	rset->beforeFirst();
	cout << "Deals:" << endl;

	while (rset->next()) {
		cout << "Deal Number: " << rset->getUInt("deal_num") << ". \tDeal Date: "
			<< rset->getString("deal_date") << "." << endl;
	}

	delete con;
	delete pstmt;
	delete rset;
}


void Database::booksOnDiscount(){
	int counter = 1;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	Statement *stmt = con->createStatement();
	rset = stmt->executeQuery("SELECT * FROM book WHERE book.global_discount > 0;");

	rset->beforeFirst();
	cout << "Books On Discout:" << endl;

	while (rset->next()) {

		cout << counter << ") " << rset->getString("name") << ", By "
			 << rset->getString("Author") << ",\tIs On A " << rset->getDouble("global_discount") * 100 << "% Discount." << endl;
		++counter;
	}

	delete con;
	delete stmt;
	delete rset;
}

void Database::checkStock() {

	string bookName;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT * FROM book WHERE book.name = ?;");
		
	cout << "Please Enter Book Name:> ";
	clearCin();
	getline(cin, bookName);
	pstmt->setString(1, bookName);
	rset = pstmt->executeQuery();

	rset->beforeFirst();
	
	if (rset->next()) {
		int stock = rset->getInt("current_stock");
		cout << "The Book '" << rset->getString("name") << "' Is Currently ";

		if (stock) {
			cout << "In Stock With " << stock << " Units Remaining!" << endl;
		}
		else cout << " NOT In Stock!" << endl;
	}

	else cout << "Book Named '" << bookName << "' Was Not Found In The Store..." << endl;

	delete con;
	delete pstmt;
	delete rset;
}

void Database::bookSupplier(){

	string bookName;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("CREATE TABLE temp AS SELECT * FROM book inner join supplier_book WHERE book.name = supplier_book.book_name AND supplier_book.book_name = ? group by supplier_num;");

	cout << "Please Enter Book Name:> ";
	clearCin();
	getline(cin, bookName);
	pstmt->setString(1, bookName);
	pstmt->execute();
	pstmt = con->prepareStatement("SELECT supplier.supplier_num, supplier_name FROM temp inner join supplier where temp.supplier_num = supplier.supplier_num;");
	rset = pstmt->executeQuery();

	rset->beforeFirst();

	cout << "The Book '" << bookName << "' Is Supplied By: " << endl;
	while (rset->next()) {
		cout << "Supplier Number: " << rset->getString("supplier_num") << ". Supplier Name: " << rset->getString("supplier_name") << "." << endl;
	}

	pstmt = con->prepareStatement("DROP TABLE temp");
	pstmt->execute();

	delete con;
	delete pstmt;
	delete rset;
}

void Database::bookSoldSince() {

	string temp;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT * FROM deal INNER JOIN deal_book WHERE deal.deal_num = deal_book.deal_num AND book_name = ? AND deal_date >= ?;");

	cout << "Please Enter Book Name:> ";
	clearCin();
	getline(cin, temp);
	pstmt->setString(1, temp);

	cout << "Please Enter Date:> ";
	getline(cin, temp);
	pstmt->setString(2, temp);

	rset = pstmt->executeQuery();
	
	rset->beforeFirst();
	if (rset->next()) {
		cout << "The Book '" << rset->getString("book_name") << "' Was Ordered " << rset->rowsCount() << " Times Since " << temp << endl;
	}

	else cout << "Invaid Book Name Or Date." << endl;

	delete con;
	delete pstmt;
	delete rset;
}


void Database::booksClientBoughtSince() {
	
	string temp;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT * FROM deal INNER JOIN deal_book WHERE deal.deal_num = deal_book.deal_num AND deal.client_id = ? AND deal.deal_date >= ?;");

	cout << "Please Enter Client Id:> ";
	clearCin();
	getline(cin, temp);
	pstmt->setString(1, temp);

	cout << "Please Enter Date:> ";
	getline(cin, temp);
	pstmt->setString(2, temp);

	rset = pstmt->executeQuery();

	rset->beforeFirst();

	if (rset->next()) {

		pstmt = con->prepareStatement("SELECT first_name, last_name FROM client WHERE client_id = ?;");
		string test = rset->getString("client_id");
		pstmt->setString(1,test);
		ResultSet *rset2 = pstmt->executeQuery();
		
		if (rset2->first()) {
			cout << rset2->getString("first_name") << " " << rset2->getString("last_name") << " Has Ordered " << rset->rowsCount() << " Books Since " << temp << endl;
		}
	}

	else cout << "Invaid Client ID Or Date." << endl;

	delete con;
	delete pstmt;
	delete rset;
}

void Database::clientsJoinedAfterDate() {
	
	string temp;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT * FROM client WHERE join_date >= ?;");

	cout << "Please Enter Date:> ";
	clearCin();
	getline(cin, temp);
	pstmt->setString(1, temp);
	rset = pstmt->executeQuery();

	if (!rset->first()) {
		cout << "Invaid Date or No Clients Found That Joined After The Date That Was Given." << endl;
		delete con;
		delete pstmt;
		delete rset;
		return;
	}

	rset->beforeFirst();
	int count = 1;

	while (rset->next()) {
		cout << count << ") " << rset->getString("first_name") << " " << rset->getString("last_name") << ",\tJoined At " << rset->getString("join_date") << "." << endl;
		++count;
	}

	delete con;
	delete pstmt;
	delete rset;
}

void Database::topClient() {

	string temp;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT client.first_name, client.last_name, SUM(deal_books_count.count_books) as counter FROM client INNER JOIN(SELECT deal.client_id, COUNT(*) AS count_books FROM deal INNER JOIN deal_book WHERE deal_book.deal_num = deal.deal_num AND deal.deal_date >= ? GROUP BY deal.deal_num) AS deal_books_count ON deal_books_count.client_id = client.client_id group by first_name ORDER BY counter desc;");
	cout << "Please Enter Date:> ";
	clearCin();
	getline(cin, temp);
	pstmt->setString(1, temp);
	rset = pstmt->executeQuery();

	if (rset->first()){
		cout << rset->getString("first_name") << " " << rset->getString("last_name") << ", Has Ordered The Most Books (" << rset->getUInt("counter") << ") Since " << temp << endl;
	}

	else cout << "Invaid Date or No Clients Found That Ordered Any Books After The Date That Was Given." << endl;
	

	delete con;
	delete pstmt;
	delete rset;
}

void Database::ordersSince() {

	string temp1 , temp2;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT * FROM orders where order_date >= ? AND order_date <= ?;");

	cout << "Please Enter Start Date:> ";
	clearCin();
	getline(cin, temp1);
	pstmt->setString(1, temp1);
	cout << "Please Enter End Date:> ";
	getline(cin, temp2);
	pstmt->setString(2, temp2);
	rset = pstmt->executeQuery();

	if (rset->first()) {
		cout << "Number Of Orders Between " << temp1 << " And " << temp2 << " Is: " << rset->rowsCount() << "." << endl;
	}
	else cout << "Invaid Date or No Orders Found That Where Ordered Between Dates." << endl;


	delete con;
	delete pstmt;
	delete rset;
}

void Database::workersSales() {

	string temp1, temp2;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT first_name, last_name, SUM(deal_val) as sales FROM deal inner join worker where worker.id = ? AND emp_id = ? AND deal.deal_date >= ? AND deal.deal_date <= ? AND is_canceled = false;");

	
	cout << "Please Enter Worker ID:> ";
	clearCin();
	getline(cin, temp1);
	pstmt->setString(1, temp1);
	pstmt->setString(2, temp1);

	cout << "Please Enter Start Date:> ";
	getline(cin, temp1);
	pstmt->setString(3, temp1);

	cout << "Please Enter End Date:> ";
	getline(cin, temp2);
	pstmt->setString(4, temp2);

	rset = pstmt->executeQuery();
	rset->first();

	string first = rset->getString("first_name");
	string last = rset->getString("last_name");

	if (!(first == "") && !(last == "")) {
		cout << "The Total Sum Of Sales By " << first << " " << last << " Between " << temp1 << " And " << temp2 << " Is: " << rset->getUInt("sales") << "$." << endl;
	}
	else cout << "Invaid Dates Or Worker ID." << endl;


	delete con;
	delete pstmt;
	delete rset;

}

void Database::topSupplierOrder() {
	string temp;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT supplier.supplier_num, supplier.supplier_name, SUM(order_books_count.count_books) as counter	FROM supplier INNER JOIN (SELECT orders.supplier_num, COUNT(*) AS count_books FROM orders "
													 "INNER JOIN order_book WHERE order_book.order_num = orders.order_num AND orders.order_date >= ? "
													 "GROUP BY orders.order_num) AS order_books_count WHERE order_books_count.supplier_num = supplier.supplier_num "
													 "group by supplier_num ORDER BY counter desc;");
	

	cout << "Please Enter A Date:> ";
	clearCin();
	getline(cin, temp);
	pstmt->setString(1, temp);

	rset = pstmt->executeQuery();
	rset->first();

	if (rset->first()) {
		cout << rset->getString("supplier_name") << " Has Supplied " << rset->getString("counter") << " Books Since " << temp << ". More Than Any Other Supplier."<< endl;
	}
	else cout << "Invaid Date." << endl;

	delete con;
	delete pstmt;
	delete rset;
}

void Database::totalDiscount() {

	string temp;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT first_name, last_name, SUM(ceiling(deal_val * deal.discount)) AS total_discount FROM deal INNER JOIN client WHERE "
													 "deal_date >= ? AND deal.client_id = ? AND client.client_id = deal.client_id ;");

	
	cout << "Please Enter Client ID:> ";
	clearCin();
	getline(cin, temp);
	pstmt->setString(2, temp);

	cout << "Please Enter A Date:> ";
	getline(cin, temp);
	pstmt->setString(1, temp);

	rset = pstmt->executeQuery();
	rset->first();

	if (rset->first()) {
		cout << rset->getString("first_name") << " " << rset->getString("last_name")<< " Has Received A Total Discount Of " << rset->getUInt("total_discount") << "$ Since " << temp << "." << endl;
	}
	else cout << "Invaid Date Or Client ID." << endl;

	delete con;
	delete pstmt;
	delete rset;

}

void Database::top10Books() {
	
	string temp1, temp2;
	int counter = 1;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT deal_book.book_name, COUNT(*) AS book_count FROM deal INNER JOIN deal_book WHERE deal.deal_num = deal_book.deal_num "
													 "AND deal.deal_date >= ? AND deal.deal_date <= ? AND deal.is_canceled = false group by book_name ORDER BY book_count DESC;");

	cout << "Please Enter Start Date:> ";
	clearCin();
	getline(cin, temp1);
	pstmt->setString(1, temp1);

	cout << "Please Enter End Date:> ";
	getline(cin, temp2);
	pstmt->setString(2, temp2);

	rset = pstmt->executeQuery();

	if (rset->rowsCount() == 0) {
		 cout << "Invaid Dates Or No Deals Where Found Between Given Dates." << endl;
		 return;
	}

	rset->beforeFirst();
	cout << "Top Books Sold Between " << temp1 << " And " << temp2 << " Are:" << endl;

	while (rset->next()) {
		cout << counter << ") " << rset->getString("book_name") << ", Selling " << rset->getString("book_count") << " Books." << endl;
		++counter;
	}
	

	delete con;
	delete pstmt;
	delete rset;
}

void Database::supplierPurchases() {

	string temp1, temp2, supplier;
	int counter = 1, sum = 0;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT book_name, supplier_price FROM order_book INNER JOIN orders INNER JOIN book WHERE order_book.book_name = book.name "
													 "AND orders.order_date BETWEEN ? AND ? AND orders.order_num = order_book.order_num AND orders.supplier_num = ?;");
	
	PreparedStatement *pstmt2 = con->prepareStatement("SELECT supplier_name FROM supplier WHERE supplier.supplier_num = ?;");
	
	cout << "Please Enter Supplier Number:> ";
	clearCin();
	getline(cin, supplier);
	pstmt->setString(3, supplier);
	pstmt2->setString(1, supplier);

	cout << "Please Enter Start Date:> ";
	getline(cin, temp1);
	pstmt->setString(1, temp1);

	cout << "Please Enter End Date:> ";
	getline(cin, temp2);
	pstmt->setString(2, temp2);

	rset = pstmt2->executeQuery();

	if (rset->rowsCount() == 0) {
		cout << "Invalid Supplier Number." << endl;
		return;
	}

	rset->first();
	supplier = rset->getString("supplier_name");
	;

	rset = pstmt->executeQuery();
	if (rset->rowsCount() == 0) {
		cout << "Invaid Dates Or No Orders Where Found Between Given Dates." << endl;
		return;
	}

	cout << "\nTotal Purchases From " << supplier << " Between " << temp1 << " And " << temp2 << ":" << endl;

	rset->beforeFirst();

	while (rset->next()) {
		sum += rset->getUInt("supplier_price");
		cout << counter << ") " << rset->getString("book_name") << ", Costing " << rset->getString("supplier_price") << "$." << endl;
		++counter;
	}
	cout << "\nSub-Total: " << sum << "$." << endl;

	delete con;
	delete pstmt;
	delete pstmt2;
	delete rset;
}

void Database::ordeToDeal() {
	
	string temp1, temp2;
	int counter = 1;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT orders.order_num, orders.order_date, first_name, last_name FROM orders inner join order_book inner join client WHERE order_date BETWEEN ? AND ? AND order_status = 'Closed'"
													 "AND orders.order_num = order_book.order_num AND orders.client_id = client.client_id group by order_num;");

	cout << "Please Enter Start Date:> ";	
	clearCin();
	getline(cin, temp1);
	pstmt->setString(1, temp1);

	cout << "Please Enter End Date:> ";
	getline(cin, temp2);
	pstmt->setString(2, temp2);

	rset = pstmt->executeQuery();
	rset->beforeFirst();

	cout << "Orders That Turned Into A Deal Between: " << temp1 << " And " << temp2 << ":" << endl;

	while (rset->next()) {
		cout << counter << ") Ordered for: " << rset->getString("first_name") << " " << rset->getString("last_name") << " At: " << rset->getString("order_date") << endl;
		++counter;
	}
	
	delete con;
	delete pstmt;
	delete rset;
}

void Database::quarterProfit() {
	
	string temp1, temp2;
	int quarter = 0, sum = 0;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("SELECT deal_val, SUM(book.supplier_price) AS price FROM deal INNER JOIN deal_book INNER JOIN book WHERE deal_date BETWEEN ? AND ? "
													 "AND deal.deal_num = deal_book.deal_num AND book.name = deal_book.book_name group by deal.deal_num;");

	cout << "Please Enter Year:> ";
	clearCin();
	getline(cin, temp1);

	cout << "Please Enter Quarter Number (1-4):> ";
	cin >> quarter;

	if (quarter != 1 && quarter != 2 && quarter != 3 && quarter != 4) {
		cout << "Bad Quarter Input.";
		delete con;
		delete pstmt;

		return;
	}

	switch (quarter) {
		
		case 1:{

			temp2 = temp1 + "-01-01";
			pstmt->setString(1, temp2);
			temp2 = temp1 + "-03-30";
			pstmt->setString(2, temp2);
			break;
		}

		case 2:{
			temp2 = temp1 + "-04-01";
			pstmt->setString(1, temp2);
			temp2 = temp1 + "-06-30";
			pstmt->setString(2, temp2);
			break;
		}

		case 3:{
			temp2 = temp1 + "-07-01";
			pstmt->setString(1, temp2);
			temp2 = temp1 + "-09-30";
			pstmt->setString(2, temp2);
			break;
		}

		case 4:{
			temp2 = temp1 + "-10-01";
			pstmt->setString(1, temp2);
			temp2 = temp1 + "-12-30";
			pstmt->setString(2, temp2);
			break;
		}
	}

	rset = pstmt->executeQuery();

	if (!rset->rowsCount()) {
		cout << "Bad Year Or Quarter Input Or No Deals Where Found In Given Year/Quarter." << endl;
		return;
	}

	rset->beforeFirst();


	while (rset->next()) {
		sum += rset->getUInt("deal_val") - rset->getUInt("price");
	}

	cout << "Total Profits In Quarter " << quarter << " Of " << temp1 << ": " << sum  << "$."<< endl;

	delete con;
	delete pstmt;
	delete rset;	
}