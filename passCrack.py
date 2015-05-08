import sys
import hashlib
import mincemeat

enumdata=[]
datasource={}
hex_str = sys.argv[1]
print 'Please wait...'

def enumerate(length, possibles):
  ret = []
  if length == 1:
    return list(possibles)
  else:
    subs = enumerate(length -1, possibles)
    for ch in possibles:
      for sub in subs:
        ret.append(str(ch) + str(sub))
  return ret

def mapfn(k,v):
 list1={}
 import md5
 v_split=v.split()
 a=v_split[-1]
 for list1 in v_split:
   list1=list1.strip()
   hashStr=md5.new(list1).hexdigest()
   if hashStr[:5]==a:
     yield list1,a

def reducefn(k,vs):
	return vs

one=enumerate(1,"abcdefghijklmnopqrstuvwxyz0123456789")
two=enumerate(2,"abcdefghijklmnopqrstuvwxyz0123456789")
three=enumerate(3,"abcdefghijklmnopqrstuvwxyz0123456789")
four=enumerate(4,"abcdefghijklmnopqrstuvwxyz0123456789")


enumdata= one + two + three+ four
temp = ''
counter = 0
for line in enumdata:
  temp = temp + line.rstrip() + ' '
  if counter % 100000 == 0:
    temp = temp + hex_str
    datasource[counter] = temp
    temp = ''
  counter += 1
  
datasource[counter] = temp

s=mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn =reducefn

results = s.run_server(password="changeme")
print results.keys()
