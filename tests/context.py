import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'pipelines')))

import pipelines
import dependencies


"""
This file sets the context for the test. 
Each test file calls this, so that they do not have to modify path individually
"""
