#importing mincemeat and sys. Also, importing division to round off the numbers
from __future__ import division
import mincemeat
import sys

#Accepting the file name as a command line argument
inp_file = sys.argv[1]
 
file = open(inp_file,'r')
dataraw = list(file)
file.close()
 
#Increasing the size of the chunks to be sent to mappers so that the number of jobs and hence the communication overhead is reduced
 temp = ''
counter = 0
datasource = {}
for line in dataraw:
  temp = temp +  line.rstrip() + ' '
  if counter % 300 == 0:
    datasource[counter] = temp
    temp = ''
  counter += 1
datasource[counter] = temp
 
#Map function. Splits the text to individual characters and yields them to the reduce function
def mapfn(k, v):
    for word in v.split():
      word = word.strip().lower()
      for chr in list(word):
        yield chr,1
        yield 'num_of_chars',1

#Reduce function. Sums up the individual character count as well as the total number of characters in the document	
def reducefn(k, vs):
    result = sum(vs)
    return result

#Starting the server and calling the map and reduce functions
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn
 
results = s.run_server(password="changeme")
total = results['num_of_chars']
 
resultlist = []

#Rounding off the results to 2 decimal places 
for key in results.keys():
    if key!= 'num_of_chars':
        per = results[key]*100/total
        per = round(per, 2)
        resultlist.append((key,results[key],str(per)+"%"))
 
resultlist = sorted(resultlist, key=lambda a: a[1])
 
print resultlist
