from __future__ import division

import datetime

import calculators
import aggregators

from gearman import GearmanWorker
from gearman import GearmanClient
from gearman.worker import POLL_TIMEOUT_IN_SECONDS
from cassandra.cluster import Cluster

from itertools import islice

import pickle
import json
import decimal

import requests

from gearman_servers import JOBSERVER_LIST


def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))

class DimWithCol(object):
    def __init__(self, dim_url, dim_col):
        self.dim_url = dim_url
        self.dim_col = dim_col
    
    def __repr__(self):
        return self.dim_url + " - " + self.dim_col
    
def add(worker, job):
    data = job.data
    print "adding: ", str(data)
    return "asd"

def task_update_callback(task_id, value):
    s = requests.Session()
    payload = {}
    payload["id"] = int(task_id)
    payload["increment"] = value
    headers = {'content-type': 'application/json'}
    payload = json.dumps(payload)
    req = s.post("http://localhost:8001/api/v1/task_update/", data=payload, headers=headers)
    print req.text
    print req.status_code


def pre_schedule(worker, job):
    """ Imports metadata from API """
    data = pickle.loads(job.data)
    #print data
    s = requests.Session()
    
    task_r = s.get("http://localhost:8001"+data["task"])
    print "doing:", data["task"]
    task_js = json.loads(task_r.text)
    
    function = task_js["async_function"]
    
    input_dataset = json.loads(s.get("http://localhost:8001"+data["input_dataset"]).text)
    output_dataset = json.loads(s.get("http://localhost:8001"+data["output_dataset"]).text)
    
    input_dataset_id = input_dataset["id"]
    output_dataset_id = output_dataset["id"]
    
    
    #print input_columns
    #print output_column
    #print function
    #print input_dataset
    #print output_dataset
    
    
    #schedule
    count = input_dataset["datapoint_count"]
    
    #TODO bucket list is "hardcoded" here. find some other way to do that
    bucket_list = ",".join(["'"+str(input_dataset_id)+"-"+str(c)+"'" for c in xrange(1+int(count/10000))])
    
    
    table = "tsstore"
    
    lowest_time = input_dataset["lowest_ts"]
    highest_time = input_dataset["highest_ts"]
    
    #partition time buckets
    date_format = "%Y-%m-%d %H:%M:%S.%f"
    lt = datetime.datetime.strptime(lowest_time, date_format)-datetime.timedelta(microseconds=1)
    ht = datetime.datetime.strptime(highest_time, date_format)
    diff = ht-lt
    
    #TODO change to some other value during production
    batch_size = 10
    
    try:
        interval = task_js["interval_in_seconds"]
        #is aggregation
        print "is aggregation"
        IS_CALCULATION = False
        num_tasks = int(diff.total_seconds()/interval)
        
        cols = []
        dims_with_cols = []
        for dim in input_dataset["dimensions"]:
            col_r = s.get("http://localhost:8001"+dim)
            jreq = json.loads(col_r.text)["ts_column"]
            cols.append(jreq)
            dims_with_cols.append(DimWithCol(dim, jreq))
        
        output_column = cols[0]
        input_columns = cols[1:]
        
        
    except KeyError:
        #is calculation
        print "is calculation"
        IS_CALCULATION = True
        cluster = Cluster(data["cassandra_nodes"])
        session = cluster.connect('ws')
        stmt = "select count(*) from tsstore where bucket in (%s) and dataset=%s;" % (bucket_list, input_dataset_id)
        row = session.execute(stmt)
        count = row[0]._asdict()["count"]
        
        session.shutdown()
        
        output_dimension = s.get("http://localhost:8001"+task_js["output_dimension"])
        output_column = json.loads(output_dimension.text)["ts_column"]
        
        input_dimensions = task_js["input_dimensions"]
        input_columns = []
        for ic in input_dimensions:
            ic_r = s.get("http://localhost:8001"+ic)
            input_columns.append(json.loads(ic_r.text)["ts_column"])
        interval = (diff.total_seconds()*batch_size)/count
        num_tasks = int(count/batch_size)
    
    #print interval
    #print count/batch_size
    
    #update task_count
    task_update_callback(data["task_id"], int(num_tasks))
    
    temp_ht = lt+datetime.timedelta(seconds=interval)+datetime.timedelta(microseconds=1)
    lt -= datetime.timedelta(microseconds=1)
    lowest_time = datetime.datetime.strftime(lt, date_format)
    highest_time = datetime.datetime.strftime(temp_ht, date_format)
        
    #while True:
    for XX in xrange(num_tasks+2):
        stmt = "select %s,dataset,bucket from %s where bucket in (%s) and time >= '%s' AND time <= '%s' and dataset=%s order by dataset, time;" % (",".join(input_columns+[output_column]+(["time"] if IS_CALCULATION else [])), table, bucket_list, lowest_time, highest_time, input_dataset_id)
        print stmt
        print num_tasks+1, XX
        #create job with previous stmt
        dat = {}
        dat["stmt"] = stmt
        dat["function"] = function
        dat["output_column"] = output_column
        dat["output_dataset"] = data["output_dataset"]
        dat["task_id"] = data["task_id"]
        dat["cassandra_nodes"] = data["cassandra_nodes"]
        client = GearmanClient(JOBSERVER_LIST)
        try:
            interval = task_js["interval_in_seconds"]
            #is aggregation
            #dat["input_columns"] = input_columns+[output_column]
            dat["input_dimensions"] = dims_with_cols
                
            client.submit_job("row_aggregator", pickle.dumps(dat),background=True)
        except KeyError:
            #is calculation
            dat["output_dimension"] = task_js["output_dimension"]
            client.submit_job("row_calculator", pickle.dumps(dat),background=True)
        
        
        #update timestamps
        lt += datetime.timedelta(seconds=interval)+datetime.timedelta(microseconds=1)
        temp_ht += datetime.timedelta(seconds=interval)+datetime.timedelta(microseconds=1)
        
        lowest_time = datetime.datetime.strftime(lt, date_format)
        highest_time = datetime.datetime.strftime(temp_ht, date_format)
        print lowest_time
        print highest_time
        
        #if lt > ht:
        #    break
    
    return "a"

def row_aggregator(worker, job):
    data = pickle.loads(job.data)
    stmt = data["stmt"]
    output_dataset = data["output_dataset"]
    input_dimensions = data["input_dimensions"]
    
    print input_dimensions
    
    cluster = Cluster(data["cassandra_nodes"])
    session = cluster.connect('ws')
    rows = session.execute(stmt)
    
    if not rows:
        print "no rows to agg"
        session.shutdown()
        task_update_callback(data["task_id"], -1)
        return "False"
        
    rows = [r._asdict() for r in rows]
    
    input_columns = [i.dim_col for i in input_dimensions]
    
    row = getattr(aggregators, data["function"])(rows, input_columns)

    #print "row:", row
    
    cluster.shutdown()
    
    #print input_dimensions
    
    payload = {}
    payload["dataset"] = output_dataset
    s = requests.Session()
    
    status_code = 1
    
    while status_code != 201:
        payload["dimensions"] = {}
        payload["update"] = False
        #payload["dimensions"][output_uri] = float(val)
        #payload["dimensions"]["/api/v1/dimension/1/"] = "'"+row._asdict()["time"]+"'"
        for dim in input_dimensions:
            payload["dimensions"][dim.dim_url] = row[dim.dim_col]
        out_payload = json.dumps(payload)
        headers = {'content-type': 'application/json'}
        print payload
        req = s.post("http://localhost:8001/api/v1/datapoint/", data=out_payload, headers=headers)
        print "posted datapoint"
        print req.text
        status_code = req.status_code
    
    task_update_callback(data["task_id"], -1)
    
    print "agg inserted"
    
    return "asd"

       

def row_calculator(worker, job):
    data = pickle.loads(job.data)
    stmt = data["stmt"]
    output_dimension = data["output_dimension"]
    output_column = data["output_column"]
    output_dataset = data["output_dataset"]
    function = data["function"]
    
    s = requests.Session()
    
    cluster = Cluster(data["cassandra_nodes"])
    session = cluster.connect('ws')
    print stmt
    rows = session.execute(stmt)
    
    if not rows:
        print "no rows to calc"
        session.shutdown()
        task_update_callback(data["task_id"], -1)
        return "False"
    
    for row in rows:
        payload = {}
        payload["dataset"] = output_dataset
        payload["update"] = True
        if row._asdict()[output_column] is None or (True):
            input_dict = {}
            for k,v in row._asdict().items():
                if k == "bucket" or k == "time" or k == "dataset":
                    continue
                input_dict[k] = v
                
            val = getattr(calculators, function)(input_dict)
            output_uri = "%s" % (output_dimension,)
            #print output_uri
            #payload["dimensions"] = str('{%s: %s, %s: %s}' % ('"'+str(output_uri)+'"', val, '"/api/v1/dimension/3/"', '"'+row._asdict()["time"]+'"'))
            status_code = 1
            while status_code != 201:
                payload["dimensions"] = {}
                payload["dimensions"][output_uri] = float(val)
                payload["dimensions"]["/api/v1/dimension/1/"] = "'"+row._asdict()["time"]+"'"
                payload = json.dumps(payload)
                headers = {'content-type': 'application/json'}
                print payload
                po = s.post("http://localhost:8001/api/v1/datapoint/", data=payload, headers=headers)
                print "posted datapoint"
                print po.text
                status_code = po.status_code

    session.shutdown()
    
    task_update_callback(data["task_id"], -1)
    
    return "asd"



def echo(worker, job):
    data = pickle.loads(job.data)
    print data
    return "a"
    

def on_job_exception(self, current_job, exc_info):
    self.send_job_failure(current_job)
    import traceback
    print str(traceback.print_exc(exc_info[2]))
    return False
    
def work(self, poll_timeout=POLL_TIMEOUT_IN_SECONDS):
    """Loop indefinitely, complete tasks from all connections."""
    continue_working = True
    worker_connections = []
    

    def continue_while_connections_alive(any_activity):
        return self.after_poll(any_activity)

    # Shuffle our connections after the poll timeout
    while continue_working:
        worker_connections = self.establish_worker_connections()
        continue_working = self.poll_connections_until_stopped(worker_connections, continue_while_connections_alive, timeout=poll_timeout)
        print "continue_working"

    # If we were kicked out of the worker loop, we should shutdown all our connections
    for current_connection in worker_connections:
        current_connection.close()

GearmanWorker.on_job_exception = on_job_exception
GearmanWorker.work = work

worker = GearmanWorker(JOBSERVER_LIST)

worker.set_client_id("working_on_the_djangoroad")

worker.register_task("add", add)
worker.register_task("pre_schedule", pre_schedule)
worker.register_task("row_calculator", row_calculator)
worker.register_task("row_aggregator", row_aggregator)

worker.register_task("echo", echo)


print "working"
#print dir(worker)
#print worker.worker_abilities
worker.work()

