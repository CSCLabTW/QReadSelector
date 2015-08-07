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
 *  This program will generate the statistics of PE minimalQ.
 *  It takes the results of PEMQExtractor as its input.
 *
 *  Usage:  PEMQsta [input.1] {[input.2] [...]}
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fstream>
#include <iostream>

using namespace std;

int main(int argc, const char * argv[]) {
	if (argc < 2) {
		cerr << "Using: " << argv[0]
				<< " [input.1] {[input.2] [...]} > [output]" << endl;
		exit(1);
	}

	for (int f = 1; f < argc; f++) {
		if (access(argv[f], F_OK) == -1) {
			cerr << "File " << argv[f] << " can not be accessed." << endl;
			exit(1);
		}
	}

	fstream fin;

	int cycle = 0;
	char line[5000];
	long Q[42][42];
	long QQ[42];
	long result[42];
	long sum = 0;

	for (int i = 0; i < 100; i++) {
		for (int j = 0; j < 100; j++) {
			Q[i][j] = 0;
		}
	}

	for (int i = 0; i < 100; i++) {
		QQ[i] = 0;
		result[i] = 0;
	}

	for (int f = 1; f < argc; f++) {
		fin.open(argv[f], ios::in);
		while (fin.getline(line, sizeof(line), '\n')) {
			Q[(int) (line[0] - 33)][(int) (line[2] - 33)]++;
		}
		fin.close();
	}

	for (int i = 0; i < 42; i++) {
		for (int j = i + 1; j < 42; j++) {
			for (int k = i + 1; k < 42; k++) {
				QQ[i] += Q[j][k];
			}
		}
	}

	for (int i = 0; i < 42; i++) {
		for (int j = 0; j < 42; j++) {
			sum += Q[i][j];
		}
	}

	for (int i = 0; i < 42; i++) {
		result[i] = sum - QQ[i];
	}

	for (int i = 0; i < 42; i++) {
		cout << i << "," << result[i] << endl;
	}

	return 0;
}
