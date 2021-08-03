#!/usr/bin/env python3

import json
import sys


def tokenize():
	for line in sys.stdin:
		for token in line.split():
			yield token


if __name__ == "__main__":
	json.dump([{"K": token, "V": ""} for token in tokenize()], sys.stdout)
