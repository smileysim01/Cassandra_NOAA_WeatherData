import grpc
from concurrent import futures
import station_pb2_grpc
import cassandra
import station_pb2
import pandas as pd
from datetime import datetime
from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
cluster = Cluster(['project-5-srh4dawin-db-1', 'project-5-srh4dawin-db-2', 'project-5-srh4dawin-db-3'])
cass = cluster.connect()

class meow(station_pb2_grpc.StationServicer):
    def RecordTemps(self,station,date):
        print('hello')
        print(type(station.station),station.station)
        print(type(station.date),station.date)
        print(type(station.tmin),station.tmin)
        print(type(station.tmax),station.tmax)
        date=datetime.strptime(station.date, '%Y-%m-%d').date()
        print(type(date))
        insert_statement = cass.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, {tmin:?, tmax:?}) ")
        insert_statement.consistency_level = ConsistencyLevel.ONE
        try:
            print('inside try')
            cass.execute(insert_statement,(station.station,date,station.tmin,station.tmax))
            print('after try')
        except Exception as e:
            return station_pb2.RecordTempsReply(error=f'There was an error while inserting data :{e}')
        print('meow')
        return station_pb2.RecordTempsReply(error=f'inserted row')
    def StationMax(self,station_id,fortnut):
        try:
            max_statement = cass.prepare(f"select * from weather.stations where id = ? ")
            max_statement.consistency_level = ConsistencyLevel.THREE
            meow=pd.DataFrame(cass.execute(max_statement,(station_id.station,)))
            max_vals=[]
            for i in meow['record']:
                if i!=None:
                    max_vals.append(max(i))
            tmax=max(max_vals)
        except Exception as e:
            return station_pb2.StationMaxReply(error=f'{e}')
        return station_pb2.StationMaxReply(tmax=tmax)

def server():  
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    station_pb2_grpc.add_StationServicer_to_server(meow(),server)
    print('LISTENING ON PORT : 5440')
    server.add_insecure_port('[::]:5440')
    print('added port')
    server.start()
    server.wait_for_termination()

server()
    


       

