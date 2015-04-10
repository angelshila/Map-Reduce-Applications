import MapReduce
import sys

"""
Relational Join Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    
    if(record[0]=="MovieNames"):
        key=record[2]
    elif(record[0]=="MovieRatings"):
        key=record[1]
    value = list(record)
    mr.emit_intermediate(key,value)
    

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    
    movie_names = []
    movie_ratings = []
    ratings=[] 
    avg=[]
    
    for x in list_of_values:
        if x[0] == 'MovieNames':
            movie_names.append(x[1:4])
        else:
            movie_ratings.append(x[1:4])
            ratings.append(x[3])
    
    
    for y in movie_names:
        for x in movie_ratings:
            mr.emit(y+x)
    
    avg.append(sum(ratings)/float(len(ratings)))
    
    for o in movie_names:
        mr.emit(o[0:1]+avg)


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
