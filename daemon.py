import json
import threading
from bottle import route, run, request, abort
from xyvar import hostname,d_ID,c_ID
import subprocess 
job_popens_all={}
job_popens_live={}
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
        job_popens_all[str(t_ID)]=job_popen
        job_popens_live[str(t_ID)=job_popen
        job_popen.wait()
        del job_popens_live[str(t_ID)]
        ##call /daemon complete on master
        fileoutput.close()
         
@route('/push', method='POST')
def push_job():
    data = request.body.readline().decode('utf-8')
    print data
    print type(data)
    if not data:
        abort(400, 'No data received')
    entity = json.loads(data)
    if (entity['d_ID'] != d_ID ):
        abort(404, 'Wrong Daemon')

    if (entity['c_ID'] != c_ID ):
        abort(404, 'Wrong cluster?')
    t_ID=entity['t_ID'] #task id
    noofargs=entity['NumArgs']
    jobargs =['/bin/sh','-c'] #check if safe?
    
    for i in xrange(noofargs):
        jobargs += entity['ARG-'+str(i)]
    jobT=jobThread(jobargs,t_ID)
    jobT.start()

@route('/checktask/:t_ID', method='GET')
def check_job(t_ID):
    return_json={}
    return_json['t_ID']=str(t_ID)
    try: #check if it is running
    	job_check=job_popens_live[str(t_ID)]
    	return_val=job_check.poll()
    	if return_val==None:
    		return_json['RETURN_VAL']="INCOMPLETE"
    except KeyError:#if not runnning or never pushed
    	try:
    		job_check=job_popens_live[str(t_ID)]
    		return_val=job_check.poll()
	    	if return_val!=None:
	    		return_json['RETURN_VAL']=return_val
	    except KeyError:#if never pushed
	    	return_json['RETURN_VAL']='NOTFOUND'
    return return_json
@route('/checkd', method='GET')
def check_daemon():
    return_json={}
    return_json['d_ID']=str(d_ID)
    if job_popens_live=={}:
        return_json["NoTASKS"]=0
    else:
        return_json["NoTASKS"]=len(job_popens_live)
    return return_json





