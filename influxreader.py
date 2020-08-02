from influxdb import InfluxDBClient, DataFrameClient
import time

# release Date: 21-10-98
__version__ = 0.2

class Database:
    def __init__(self, host, port, database, username, password, sleep=5, timeout=100):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.sleep_sec = sleep      #sleep 5 sec between each query
        self.client = None
        self.timeout = timeout      # timeout for seeder if data not exist 
        
    def prepare_client(self):
        self.client = InfluxDBClient(host=self.host, port=self.port, database=self.database, 
                                     username=self.username, password=self.password)
        self.client.switch_database(self.database)
        
    def switch_database(self, database):
        self.client.switch_database(database)
        self.database = database

    def seeder(self, measurement, timestamp=None, Err_raise=False, verbose=False, epoch="s"):
        """timestamp: int number in second resolution"""
        q = f'select * from "{measurement}" where time>'
        if timestamp:
            timestamp *= 1000000000
            query = q + f"{int(timestamp)}"
        else:
            now = int(time.time())*1000000000
            query = q + f"{now}"
        if verbose:
            print(query)
        res = self.client.query(query, epoch=epoch)
        timeout = 0
        while 1:
            time.sleep(self.sleep_sec)   # sleep 5 sec between each query
            # res_q = 0
            for r in res.get_points():
                # if res_q==0:
                #     res_q+=1
                #     continue
                yield (r)
                
            try:
                next(res.get_points())
                timeout = 0
            except :
                timeout += self.sleep_sec
                print(f"TIMEOUT in: {self.timeout-timeout}(sec)  \r", end="", flush=True)
                if timeout >= self.timeout:
                    if Err_raise:
                        raise TimeoutError(f"DataPoint not exist in {self.timeout} second!")
                    else:
                        return

            try:
                query = q + f'{int((r["time"]+1)*1000000000)}'
            except:
                query = q + f"{now}"
            

            res = self.client.query(query, epoch=epoch)

          
            
if __name__ == "__main__":
    host = 'localhost'
    port = 8086
    database = "dbname"
    username = None
    password = None
    
    db = Database(host, port, database, username, password, timeout=20)
    db.prepare_client()
    
    for c in db.seeder("measurement", timestamp=1570000000, Err_raise=True):
        print(f'{c["time"]:<32} | val01:{c["val01"]:^5} | val02:{c["val02"]:^5}')