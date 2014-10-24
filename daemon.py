import json
from bottle import route, run, request, abort
from xyvar import hostname,d_ID,c_ID
@route('/run', method='POST')
def run_job():
    data = request.body.readline().decode('utf-8')
    print data
    print type(data)
    if not data:
        abort(400, 'No data received')
    entity = json.loads(data)
    if (entity['d_ID'] != d_ID )
        abort(404, 'Wrong Daemon')
    if (entity['c_ID'] != c_ID )
        abort(404, 'Wrong cluster?')

    noofargs=entity['NumArgs']
    for i in xrange(noofargs):
        jobarg[i]=entity['ARG-'+str(i)]


 
@route('/rm/:id1', method='GET')
def rm_doc(id1):
    return "Deleted"
@route('/documents/:id', method='GET')
def get_document(id):
    if not entity:
        abort(404, 'No document with id %s' % id)
    return entity
