import MapReduce
import sys

"""
Mutual Friends Example in the Simple Python MapReduce Framework
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
      if(key<v):
        #print key+v,value
        mr.emit_intermediate(key+v,value)
      else:
        #print v+key,value
        mr.emit_intermediate(v+key,value)
    
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
   
    if(len(list_of_values)>1):
      if(sorted(set(list_of_values[0]) & set(list_of_values[1]), key = list_of_values[0].index)):
        mr.emit((key,sorted(set(list_of_values[0]) & set(list_of_values[1]), key = list_of_values[0].index)))
      
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
