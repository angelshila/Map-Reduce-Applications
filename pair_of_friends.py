import MapReduce
import sys


"""
Pair of Friends Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    
    for v in value:
      if(key>v):
        #print v+key,key,v
        mr.emit_intermediate(v+key,[key,v])
      else:
        #print key+v,key,v
        mr.emit_intermediate(key+v,[key,v])
    

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    list_of_values.sort()
    if(len(list_of_values)>=2):
      mr.emit(list_of_values[0])

    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
