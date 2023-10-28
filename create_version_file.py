# Import necessary libraries
import os
import sys

# Create an empty dictionary 'about'
about = {}

# Open the "__about__.py" file and execute its content to populate 'about' dictionary
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)

# Retrieve the version from the 'about' dictionary
version = about['__version__']

# Check the command line argument to determine the filename
if sys.argv[1] == 'beta':
    filename = 'beta_version.txt'
elif sys.argv[1] == 'release':
    filename = 'stable_version.txt'

# Create directories and subdirectories if they don't exist
os.system('mkdir -p distributions/ver/dist')

# Write the 'version' to the specified filename in the distributions directory
with open(f'distributions/ver/dist/{filename}', 'w') as fp:
    fp.write(version)
