import json
from bottle import route, run, request, abort
from xyvar import hostname,d_ID,c_ID,md_ID,slaves
import subprocess 

         
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

