import glob
import re
import sys

numbers = re.compile(r'(\d+)')

def numericalSort(value):
    parts = numbers.split(value)
    parts[1::2] = map(int, parts[1::2])
    return parts

new_file = open("partition_scores.csv", "w")

for outfile in sorted(glob.glob("*.out"), key=numericalSort):
    partitions = int(outfile.split("-")[0])
    print "Finding edge cut for partition "+str(partitions)
    fh = open(outfile, "r")
    lines = fh.readlines()

    try:
        edge_cut = re.search("Edgecut: (\d+),", lines[14])
    except IndexError:
        print "Edge cut not found for "+outfile
        print lines


    new_file.write(str(partitions)+","+str(int(edge_cut.group(1))/partitions)+"\n")