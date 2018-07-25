#include "stdafx.h"
#include "Lecturer.h"



Lecturer::Lecturer(string id, string firstname, string lastname, string city, string street, unsigned int street_num, string birthdate) 
	try: id(id), firstname(firstname), lastname(lastname), city(city), street(street), street_num(street_num), birthdate(from_string(birthdate)) {}
	catch (exception &e) {
		cerr << "Something went wrong.";
	}

Lecturer::~Lecturer()
{
}

