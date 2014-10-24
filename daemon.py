import json
from bottle import route, run, request, abort
import xyvar
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
    

    if not entity.has_key('_id'):
        abort(400, 'No _id specified')
    try:
        print entity
    except ValidationError as ve:
        abort(400, str(ve)) 
@route('/rm/:id1', method='GET')
def rm_doc(id1):
    return "Deleted"
@route('/documents/:id', method='GET')
def get_document(id):
    if not entity:
        abort(404, 'No document with id %s' % id)
    return entity
