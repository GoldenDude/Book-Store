// sql tirgul.cpp : Defines the entry point for the console application.
//


#include "Database.h"
#include "Lecturer.h"
#include <iostream>

using namespace std;

int main()
{
	Lecturer *lec = Lecturer::getById("1112");
	if (lec) {
		cout << lec->firstname << endl;
	}
    return 0;
}

