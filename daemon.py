import json
import threading
from bottle import route, run, request, abort
from xyvar import hostname,d_ID,c_ID
import subprocess 
class jobThread(threading.Thread):
     def __init__(self,jobargs,t_ID):
        threading.Thread.__init__(self)
        self.jobargs=jobargs
        self.t_ID=t_ID

     def run(self):
        fileoutput=open(str(t_ID)+'.output','w')
        job_popen = subprocess.Popen(jobargs
                                    bufsize=-1,
                                    executable=None,
                                    stdin=None,
                                    stdout=fileoutput,
                                    stderr=subprocess.STDOUT, #check for deadlock Popen.communicate()
                                    shell=False,
                                    env=None,
                                    universal_newlines=False,
                                    startupinfo=None,
                                    creationflags=0)
        job_popen.wait()
        ##call /daemon complete on master
        fileoutput.close()
         
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
    t_ID=entity['t_ID'] #task id
    noofargs=entity['NumArgs']
    jobargs =['/bin/sh','-c'] #check if safe?
    
    for i in xrange(noofargs):
        jobargs += entity['ARG-'+str(i)]
    jobT=jobThread(jobargs,t_ID)
    jobT.start()


