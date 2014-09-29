# ====================================================================================================
# Rehydrate tweet ids using the twitter api. Tweets are exported in json format as an array of tweet
# objects.
# ====================================================================================================

import csv
import argparse
import os
import sys
import string

# ====================================================================================================

# --------------------------------------------------------------------------------
# Set up arguments

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input-file", dest="csvFile", metavar="FILE", required=True,
                    help="File containing tweet ids")
parser.add_argument("-o", "--output-file", dest="outFile", metavar="FILE",
                    required=False, help="Output file")
parser.add_argument("-f", "--field-num", dest="fieldNum", metavar="NUM", type=int,
                    required=True, help="1-based column number to look for ids")

args = parser.parse_args()

if not os.path.isfile(args.csvFile):
    sys.stderr.write("Input file '" + args.csvFile + "' not found\n")
    raise SystemExit

# --------------------------------------------------------------------------------
# Open up input and output files

try:
    inFd = open(args.csvFile, 'rb')
except IOError:
    sys.stderr.write("Error opening input file: '", args.csvFile, "' ", e.strerror, "\n")
    raise SystemExit
    
# open up the output stream

if ( None == args.outFile ):
    outFd = sys.stdout
else:
    try:
        outFd = open(args.outFile, 'w')
    except IOError:
        sys.stderr.write("Error opening output file: '" + args.outFile + "' " + e.strerror + "\n")
        raise SystemExit

column = args.fieldNum - 1

# --------------------------------------------------------------------------------
# Read the CSV file

dataset = csv.reader(inFd, dialect='excel')
dataset.next()
for row in dataset:
    value = row[column]
    outFd.write(value[str.rfind(value, '/') + 1:] + "\n")

inFd.close()
if ( None != args.outFile ):
    outFd.close()

raise SystemExit
