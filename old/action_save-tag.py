#!/usr/bin/python
# encoding: utf-8
import sys
import os.path
from workflow import Workflow

def main(wf):
	# Get user input
	_input = wf.args[0]

	# Write the inputted Tag key to a temporary file
	with open(wf.cachefile(u"tag_query_result.txt"), 'w') as f:
		f.write(_input.encode('utf-8'))
		f.close()

if __name__ == '__main__':
	wf = Workflow(libraries=[os.path.join(os.path.dirname(__file__), 'dependencies')])
	sys.exit(wf.run(main))

   