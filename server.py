#rgundavarapu@wisc.edu
import grpc
from concurrent import futures
import station_pb2_grpc
import station_pb2
import threading
import collections
mylock=threading.Lock()

my_cache=collections.OrderedDict()# implementing LRU, a google search for whether python dicts are ordered, gave me this idea, article link is https://medium.com/junior-dev/python-dictionaries-are-ordered-now-but-how-and-why-5d5a40ee327f
cache_size=10
dict_val={} # dictionary of values
dict_sum = 0 # sum of values in the dictionary

class meow(station_pb2_grpc.StationServicer):
    def RecordTemps(self, t_high, t_low):
        
        
        
    
    
    
    
    def SetNum(self,k,val):
        global dict_sum
        global dict_val
        with mylock:
            #print('inside lock')
            if k.key in dict_val.keys():
                curr=dict_val[k.key]
                dict_val[k.key]=k.value # update the value
                dict_sum+=(k.value-curr) # update the total by calculating the diff between old and new value
            else:
                dict_val[k.key]=k.value
                dict_sum+=k.value
            #print('about to return setnum')
        return station_pb2.SetNumResponse(total=dict_sum)
    def Fact(self,factkey,factval):
        global my_cache
        global cache_size
        factorial=1
        if factkey.key in dict_val:
            mylock.acquire()
            if factkey.key in my_cache.keys():
                #print('current',my_cache)
                factorial=my_cache[factkey.key] ## return already computed value
                my_cache.move_to_end(factkey.key) # move the current key to end as its recently used
                #print('updated',my_cache)
                mylock.release()
                return station_pb2.FactResponse(value=factorial,hit=True)
            elif factkey.key not in my_cache.keys() and len(my_cache)<cache_size:  
                # if factkey is not in cache and cache has space, calculate factorial and add it to cache
                #print(my_cache)
                mylock.release() # releasing lock when computing factorial
                for i in range(1,dict_val[factkey.key]+1):
                    factorial*=i
                my_cache[factkey.key]=factorial
                #print('updated',my_cache)
                return station_pb2.FactResponse(value=factorial,hit=False)
            elif factkey.key not in my_cache.keys() and len(my_cache)==cache_size:
                print('CACHE FULL, POPPING OLDEST VALUE')
                print(my_cache)
                ## cache is full, should pop the oldest value i.e the first value in the dict
                my_cache.popitem(last = False) # remove first item i.e oldest value used
                mylock.release()
                for i in range(1,dict_val[factkey.key]+1):
                    factorial*=i # calculate factorial for this new value and store it
                my_cache[factkey.key]=factorial
                my_cache.move_to_end(factkey.key) # move the current key to end as its recently used
                #print('updated',my_cache)
                return station_pb2.FactResponse(value=factorial,hit=False)
        else:
            return station_pb2.FactResponse(error='key does not exist,please add it using setnum')
def server():  
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    station_pb2_grpc.add_StationServicer_to_server(meow(),server)
    print('LISTENING ON PORT : 5440')
    server.add_insecure_port('[::]:5440')
    print('added port')
    server.start()
    server.wait_for_termination()
    
server()

       

