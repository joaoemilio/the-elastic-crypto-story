# set default shell
SHELL = bash -e -o pipefail

# Variables
VERSION                  ?= $(shell cat VERSION)


run-download-symbol:
	python download_symbol.py BTC USDT 1m

test:
	python -m unittest



# end file
