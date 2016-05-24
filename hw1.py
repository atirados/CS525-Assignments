import csv
import pprint
import itertools
import argparse
import sys

__author__ = "Adrian Tirados Mata"
__email__ = "atirados@hawk.iit.edu"

pp = pprint.PrettyPrinter(indent=1, width=40)


def run():
    """
    Function that parses the arguments from the command line and then
    reads and prints the tuples of both files.
    Depending on the operation mode (cartesian/natural/outer),
    it calls different function to manipulate the data.
    """
    # Argument parser
    parser = argparse.ArgumentParser(
        description='This script calculates the Cartesian Product, Natural Join, or Left Outer Join of two given tables')
    parser.add_argument(
        '-m', '--mode', help='Operation mode (cartesian: Cartesian Product / natural: Natural Join / outer: Left Outer Join)', required=True)
    parser.add_argument(
        '-a', '--fileA', help='Table A .csv file', required=True)
    parser.add_argument(
        '-b', '--fileB', help='Table B .csv file', required=True)
    args = parser.parse_args()

    # Get operation mode and .csv files
    mode = args.mode
    fileA = args.fileA
    fileB = args.fileB

    # Get information from file A
    tableA = readFile(fileA)
    print "------ Table A ------"
    pp.pprint(tableA)
    attributesA = getAttributes(tableA)
    tuplesA = getTuples(tableA)
    columnsA = getSetOfColumns(attributesA)
    attMapA = getMapOfAttributes(attributesA)

    # Get information from file B
    tableB = readFile(fileB)
    print "------ Table B ------"
    pp.pprint(tableB)
    attributesB = getAttributes(tableB)
    tuplesB = getTuples(tableB)
    columnsB = getSetOfColumns(attributesB)
    attMapB = getMapOfAttributes(attributesB)

    # Calculate union and difference of attributes as a helper for natural and
    # outer joins
    union = getUnion(columnsA, columnsB)
    diff = getDiff(columnsB, union)

    # Select mode depending on user input
    if mode == 'cartesian':
        print "------ CARTESIAN PRODUCT ------"
        print attributesA + attributesB
        cartesian(tuplesA, tuplesB)
    elif mode == 'natural':
        print "----- NATURAL JOIN -----"
        print attributesA + list(diff)
        natural(tuplesA, tuplesB, attMapA, attMapB, union, diff)
    elif mode == 'outer':
        print "------ LEFT OUTER JOIN ------"
        print attributesA + list(diff)
        outer(tuplesA, tuplesB, attMapA, attMapB, union, diff)
    else:
        print "Error. Mode not supported"
        sys.exit()


def readFile(fileName):
    """
    Function that reads a .csv file and returns a list with all the
    lines that the reader gets from the file
    """
    with open(fileName, 'rb') as f:
        reader = csv.reader(f)
        table = list(reader)
        return table


def getAttributes(table):
    """
    Function that gets the attributes of a given table
    """
    return table[0]


def getTuples(table):
    """
    Function that gets the tuples of a given table
    """
    return table[1:]


def getSetOfColumns(attributes):
    """
    Function that returns a set of the attributes of the file.
    This set is useful to calculate the union and difference of
    the attributes of both tables, since Python does not allow
    this operations in lists
    """
    columns = set(attributes)
    return columns


def getMapOfAttributes(attributes):
    """
    Function that returns a map (attribute, position) of the attributes given.
    This map is useful to seek through the list of attributes when computing
    the natural and outer joins.
    """
    map = {j: i for i, j in enumerate(attributes)}
    return map


def getUnion(columnsA, columnsB):
    """
    Function that computes the union of two given sets of attributes.
    """
    return columnsA & columnsB


def getDiff(columns, union):
    """
    Function that computes the difference of two given sets of attributes.
    """
    return columns - union


def cartesian(tuplesA, tuplesB):
    """
    Function that computes the cartesian product of two given lists of tuples
    and then prints them in the standard output.
    """
    for row in itertools.product(tuplesA, tuplesB):
        print row[0] + row[1]


def natural(tuplesA, tuplesB, attMapA, attMapB, union, diff):
    """
    Function that computes the natural join of two given lists of tuples and
    then prints the results in the standard output.
    """
    for rowA in tuplesA:
        for rowB in tuplesB:
            # Check if all tuples of both rows are equal maping them with the
            # results of the attributes union
            if (all(rowA[attMapA[i]] == rowB[attMapB[i]] for i in union)):
                # Fill buffer with tuples of A
                buff = rowA[:]
                for i in diff:
                    # Append tuples of B
                    buff.append(rowB[attMapB[i]])
                print buff


def outer(tuplesA, tuplesB, attMapA, attMapB, union, diff):
    """
    Function that computes the left outer join of two given lists of tuples
    and then prints the results in the standard output.
    """
    # List that contains the results of the left outer join
    outerJoinResult = []

    # Temporal list that checks for dangling tuples
    outerJoinTemp = []
    for rowA in tuplesA:
        for rowB in tuplesB:
            # Check if all tuples of both rows are equal maping them with the
            # results of the attributes union
            if (all(rowA[attMapA[i]] == rowB[attMapB[i]] for i in union)):
                buff = rowA[:]
                buffCheck = rowA[:]
                buffCheck.append('NONE')
                # Check if there is already a dangling tuple in the list of
                # results
                if (buffCheck in outerJoinResult):
                    # If there is one, remove it and lates substitute it
                    # with a non dangling tuple
                    outerJoinResult = [
                        x for x in outerJoinResult if x != buffCheck]
                for i in diff:
                    buff.append(rowB[attMapB[i]])
                # Append buffer to the list of results
                outerJoinResult.append(buff)
                # Append just the tuples of A to the list of dangling tuples
                outerJoinTemp.append(rowA[:])
                continue
            # If not all tuples are equal, then we append a dangling tuple
            # with 'NONE' in the right (left outer join)
            else:
                buff = rowA[:]
                buff.append('NONE')
                # If we have already found a match, go to next step
                # of the loop
                if (rowA[:] in outerJoinTemp):
                    continue
                # If we have already found a dangling tuple, go to
                # next step of the loop
                if (buff in outerJoinResult):
                    continue
                outerJoinResult.append(buff)
    # Pretty Print the results
    pp.pprint(outerJoinResult)

# Execute script
run()
