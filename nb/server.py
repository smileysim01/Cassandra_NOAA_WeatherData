import grpc
from concurrent import futures
import station_pb2_grpc
import cassandra
import station_pb2
from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
cluster = Cluster(['project-5-srh4dawin-db-1', 'project-5-srh4dawin-db-2', 'project-5-srh4dawin-db-3'])
cass = cluster.connect()

class meow(station_pb2_grpc.StationServicer):
    def RecordTemps(self,station,date,tmin,tmax):
        insert_statement = cass.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, {tmin:?, tmax:?})")
        try:
            cass.execute(insert_statement,(station,date,{tmin,tmax}))
        except Exception as e:
            return station_pb2.RecordTempsReply(error=f'There was an error while inserting data :{e}')
        insert_weather_stations.consistency_level = ConsistencyLevel.ONE
    def StationMax(self,station):
        max_statement = cass.prepare(f"select MAX(record) as TMAX from weather.stations where id=={station}")
        max_statement.consistency_level = ConsistencyLevel.THREE
        tmax=cass.execute(max_statement)
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
    


       

