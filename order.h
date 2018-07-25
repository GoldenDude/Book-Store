#ifndef _LECTURER_H
#define _LECTURER_H

#include <iostream>
#include "Database.h"
#include <boost/date_time/posix_time/posix_time.hpp>

using namespace std;
using namespace boost::posix_time;
using namespace boost::gregorian;

class Lecturer
{
public:
	string id;
	string firstname;
	string lastname;
	string city;
	string street;
	unsigned int street_num;
	date birthdate;

	static Lecturer * getById(string id) {
		Database &db = Database::getInstance();
		Connection *con = db.getConnection();
		PreparedStatement *pstmt = con->prepareStatement("SELECT * FROM providers WHERE id = ?");
		pstmt->setString(1, id);
		ResultSet *rset = pstmt->executeQuery();
		rset->beforeFirst();
		if (rset->rowsCount() == 1) {
			rset->first();
			Lecturer *result = new Lecturer(
				rset->getString("id"),
				rset->getString("firstname"),
				rset->getString("lastname"),
				rset->getString("city"),
				rset->getString("street"),
				rset->getInt("street_num"),
				rset->getString("birth_date"));

			delete pstmt;
			delete rset;
			delete con;
			cout
				<< result->firstname << " " << result->lastname << endl
				<< result->birthdate << endl;
			return result;
		}



	}

	Lecturer(string id, string firstname, string lastname, string city, string street, unsigned int street_num, string birthdate);
	~Lecturer();
};

#endif
