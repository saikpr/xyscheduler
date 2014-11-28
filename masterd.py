#!/usr/bin/python
import json, httplib, time, hashlib
from bottle import route, run, request, abort
from xyvar import hostname,d_ID,c_ID,md_ID,slaves,daemon_Port,master_Port
import subprocess 
tempmd5=hashlib.md5()
connections={}
slave_status={}
live_jobs={}
completed_jobs_d={}
completed_jobs_status={}
new_jobs={}
new_jobs_id={}
max_number_tasks=2

def check_slaves():
    global connections,slave_status
    for sl_ID in slaves:
        conn=httplib.HTTPConnection(slaves[sl_ID],daemon_Port)
        conn.request("GET", "/checkd")
        tempres = conn.getresponse()
        tempdata=tempres.read()
        entity = eval(str(tempdata))
        slave_status[str(sl_ID)]=entity['NoTASKS']
        conn.close()


def jobscheduler():
    global connections
    global new_jobs,new_jobs_id
    global max_number_tasks
    global slave_status
    print new_jobs
    
    check_slaves()

    checkstat=False
    availslave=None
    for oneslave in slave_status:
        if  slave_status[str(oneslave)]<=max_number_tasks:
            checkstat=True
            availslave=str(oneslave)
            break

    if checkstat==False:
        return -1
    try :
        tempjobid, tempjob = new_jobs.popitem()
    except KeyError:
        print "empty"
        return
    tempjob['d_ID']=availslave
    tempjob['c_ID']=str(c_ID)
    #del new_jobs[str(tempjobid)]
    live_jobs[str(tempjobid)]=tempjob
    conn=httplib.HTTPConnection(slaves[availslave],daemon_Port)
    conn.request("POST", "/push",str(tempjob))
    response=conn.getresponse()
    print response.status, response.reason
    conn.close()
def checkjob(tem_t_ID,tem_d_ID): #not yet
    tempres=None
    global connections
    temp=connections[tem_d_ID].request("GET", "/checktask/"+str(tem_t_ID))
    tempres = temp.getresponse()
    tempdata=tempres.read()
    entity = eval(str(tempdata))
    temp.close()
    return tempdata['RETURN_VAL']


@route('/addjob', method='POST')
def add_job():
    global new_jobs,slave_status
    data = request.body.readline().decode('utf-8')
    #print data
    #print type(data)
    if not data:
        abort(400, 'No data received')
    entity = eval(str(data))
    #print(str(entity))
    ent=eval(str(entity))
    #print ent
    if (ent['c_ID'] != c_ID ):
        abort(404, 'Wrong cluster?')
    temptime=time.time()
    global tempmd5
    tempmd5.update(str(temptime))
    #print tempmd5.hexdigest()
    ent['t_ID']=tempmd5.hexdigest()
    new_jobs[ent['t_ID']]=ent
    print ent
    jobscheduler()
    print slave_status
    return tempmd5.hexdigest()

@route('/daemondone/:t_ID', method='POST')
def push_job(t_ID):
    global completed_jobs_d,live_jobs,completed_jobs_status

    data = request.body.readline().decode('utf-8')
    print data
    #print type(data)
    if not data:
        abort(400, 'No data received')
    entity = eval(str(data))
    if (entity['c_ID'] != c_ID ):
        abort(404, 'Wrong cluster?')
    if t_ID!=entity['t_ID']:
        abort(405, 'Some Error')
    del live_jobs[str(t_ID)]
    completed_jobs_d[str(t_ID)]=entity['d_ID']
    completed_jobs_status[str(t_ID)]=entity['RETURN_VAL']
    jobscheduler()
    
if __name__=="__main__":
    for sl_ID in slaves:
        connections[sl_ID]=httplib.HTTPConnection(slaves[sl_ID],daemon_Port)

    run(host='localhost',port=master_Port)