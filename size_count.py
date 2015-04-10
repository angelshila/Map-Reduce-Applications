import MapReduce
import sys

"""
Size Count in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    large=0
    medium=0
    small=0
    tiny=0
    for w in words:
      if (len(w)>=10):
        large+=1
      
      elif (len(w)>=5 and len(w)<=9):
        medium+=1
        
      elif (len(w)>=2 and len(w)<=4):
        small+=1
        
      elif (len(w)==1):
        tiny+=1
    
    
    mr.emit_intermediate(key,[["large",large],["medium",medium],["small",small],["tiny",tiny]])

  
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    for v in list_of_values:
      
      mr.emit((key,v))
    
    
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
