#importing libraries
import pandas as pd
import random
import sys

#reading input
#path = r'D:\Nidhi\MA\MA\SEM-6\BDP\data2.txt'
path = sys.argv[1]
X = pd.read_csv(path, sep=" ", header=None)

#ask for iterations
iterations = int(input("Enter number of iterations: "))

#Number of vertices
min_v = min(list(X.min()))
v = max(list(X.max()))

#Number of Edges
e = len(X[0])
edges_left = e

#initialising minimum_cut to a large value
min_cut = 100000
final_adj = {}
final_parent = {}
for iterr in range(iterations):
    #initialising adjacency list for each iteration
    adj = {}
    for i in range(min_v,v+1):
        adj[i]=[]

    #initialising parent dict for each iteration
    parent = {}
    for i in range(min_v,v+1):
        parent[i]=[i]
        
    #preparing adjacency list for each iteration
    for i in range(e):
        adj[X[0][i]].append(X[1][i])
        adj[X[1][i]].append(X[0][i])
        
    Y = X.copy()
        
    while( len(parent)>2 ):
        #picking out a random edge
        j = random.randint(0,len(Y[0])-1)
        #print("random number generated = ", j)
        #defining vertices to be merged as v1 and v2
        v1 = Y[0][j]
        v2 = Y[1][j]
        #follow the operations such that vertices joined do not come again
        Y = Y.drop(j)
        Y = Y.reset_index()
        Y = Y.iloc[:,1:]
        Y = Y.replace({0:{v2:v1}, 1:{v2:v1}})
        #print(v1, v2)
        
        if(v1!=v2):
            #merge v1 and v2
            adj[v1].extend(adj[v2])
            for k in adj[v2]:
            #check if more than one occurences of v2
                adj[k].remove(v2)
                adj[k].append(v1)
            #remove self loops from the adjacency list of v1
            adj[v1] = [x for x in adj[v1] if x!=v1]
            #del vertex v1
            del adj[v2]
            #updating parent dictionary
            parent[v1].extend(parent.pop(v2))
    
    #comparing minimum cut value across iterations
    first = list(adj.keys())
    #print(len(adj[first[0]]))
    if(min_cut > len(adj[first[0]])):
        min_cut = len(adj[first[0]])
        final_adj = adj.copy()
        final_parent = parent.copy()
    #print("list",final_parent)
    

#print min cut value
first = list(final_adj.keys())
print("Min Cut Value = ",len(final_adj[first[0]]))

#print communities
first = list(final_parent.keys())
for i in final_parent[first[0]]:
    print(i,1)
for i in final_parent[first[1]]:
    print(i,2)
        
    
    


    
    
    
    
    
    
    
    