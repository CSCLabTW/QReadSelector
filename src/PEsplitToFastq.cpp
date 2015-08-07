/*
 *  QReadSelector: subset selection of high-depth NGS reads for de novo assembly
 *  Copyright (C) 2015  The QReadSelector project, Academia Sinica, Taiwan.
 *
 *  This file is part of QReadSelector.
 *
 *  QReadSelector is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 *  This program will split a PE record into two reads, R1 and R2.
 *  It takes the result of PEMinimalQFilter as its input, and produces fastq format output.
 *
 *  Usage:  PEsplitToFastq [input] [output.R1 (fastq)] [output.R2 (fastq)]
 */

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;

int main(int argc, const char * argv[]) {
	if (argc < 4) {
		cerr << "Using: " << argv[0]
				<< " [input] [output.R1 (fastq)] [output.R2 (fastq)]" << endl;
		exit(1);
	}

	const char *filename1 = argv[1];
	const char *filename2 = argv[2];
	const char *filename3 = argv[3];

	fstream fin;
	fstream out1, out2;

	int cycle = 0;
	char line[5000];
	int count = 0;

	out1.open(filename2, ios::out);
	out2.open(filename3, ios::out);

	fin.open(filename1, ios::in);
	while (fin.getline(line, sizeof(line), '\n')) {
		switch (count) {
		case 0:
		case 1:
		case 2:
		case 3:
			out1 << line << endl;
			count++;
			break;
		case 4:
		case 5:
		case 6:
		case 7:
			out2 << line << endl;
			if (count == 7) {
				count = 0;
			} else {
				count++;
			}
			break;
		}
	}
	fin.close();

	out1.close();
	out2.close();

	return 0;
}

