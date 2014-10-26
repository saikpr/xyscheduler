import json, httplib
from bottle import route, run, request, abort
from xyvar import hostname,d_ID,c_ID,md_ID,slaves,daemon_Port
import subprocess 
connections={}
slave_status={}
live_jobs={}
completed_jobs_d={}
completed_jobs_status={}
remaining_jobs={}
def check_slaves():
    for sl in slaves:
        temp=connections[sl].request("GET", "/checkd")
        tempres = temp.getresponse()
        tempdata=tempres.read()
        entity = json.loads(tempdata)
        slave_status[str(sl)]=entity['NoTASKS']
        temp.close()
         
@route('/daemondone/:t_ID', method='POST')
def push_job(t_ID):
    data = request.body.readline().decode('utf-8')
    print data
    print type(data)
    if not data:
        abort(400, 'No data received')
    entity = json.loads(data)
    if (entity['c_ID'] != c_ID ):
        abort(404, 'Wrong cluster?')
    if t_ID!=entity['t_ID']:
        abort(405, 'Some Error')
    del live_jobs[str(t_ID)]
    completed_jobs_d[str(t_ID)]=entity('d_ID')
    completed_jobs_status[str(t_ID)]=entity('RETURN_VAL')

if __name__="__main__":
    for sl_ID in slaves:
        connections[sl_ID]=httplib.HTTPConnection(slaves[sl_ID],daemon_Port)

