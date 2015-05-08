#importing mincemeat and sys
import mincemeat
import sys

#Accepting the file name as a command line argument and reading the data from it
inp_file = sys.argv[1]
file = open(inp_file,'r')
data = list(file)
file.close()

#Defining a counter to count the number of numbers in the file
counter = 0


# The data source can be any dictionary-like object
datasource = dict(enumerate(data))

#map function. Passing the lists with tags to the reduce function
def mapfn(k, v):
	yield 'count', 1
	yield 'sum', v
	yield 'stddev', int(v)

#reduce function. Performs all the necessary calculations and stores them against the tag
def reducefn(k, vs):
	import numpy
	result = 0
	counter = 0
	std_dev = 0
	if k == 'count':
		for num in vs:
			counter = counter + 1
			
		return counter
		
	if k == 'sum':
		for num in vs:
			result = result + int(num)
		
		return result
			
	if k == 'stddev':
		std_dev = numpy.std(vs)
		return std_dev

#Starting the server and calling the map and reduce functions
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

res = s.run_server(password="changeme")

#Displaying the results
print res
