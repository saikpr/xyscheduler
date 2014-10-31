import json, httplib
import threading
from bottle import route, run, request, abort
from xyvar import hostname,d_ID,c_ID,master_ip,master_Port
import subprocess 
job_popens_all=None   #keeps all job popens
job_popens_live=None  #keeps the live jobs popens
master_connect=httplib.HTTPConnection(master_ip,master_Port) #create the connection to master
class jobThread(threading.Thread): #this is a job thread which runs to 
     def __init__(self,jobargs,t_ID):
        threading.Thread.__init__(self)
        self.jobargs=jobargs
        self.t_ID=t_ID

     def run(self):
        global job_popens_all,job_popens_live,master_connect
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
        del job_popens_live[str(t_ID)] #removing the closed jobs
        ##call /daemon complete on master
        fileoutput.close()
        return_json={}
        return_json['t_ID']=str(t_ID)
        return_json['d_ID']=str(d_ID)
        return_json['c_ID']=str(c_ID)
        return_json['RETURN_VAL']=job_popen.poll()
        temp=master_connect.request("POST", "/daemondone/"+str(t_ID),return_json)
        tempres = temp.getresponse()
        temp.close()

         
@route('/push', method='POST')
def push_job():
    data = request.body.readline().decode('utf-8')
    #print data
    #print type(data)
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
    global job_popens_live
    return_json={}
    return_json['t_ID']=str(t_ID)
    try: #check if it is running
    	job_check=job_popens_live[str(t_ID)]
    	return_val=job_check.poll()
    	if return_val==None:
    		return_json['RETURN_VAL']="INCOMPLETE"
    except KeyError:#if not runnning or never pushed i.e. the key does not exit
    	try:
    		job_check=job_popens_live[str(t_ID)]
    		return_val=job_check.poll()
	    	if return_val!=None:
	    		return_json['RETURN_VAL']=return_val
	    except KeyError:#if never pushed i.e. the key does not exit
	    	return_json['RETURN_VAL']='NOTFOUND'
    return return_json
@route('/checkd', method='GET')
def check_daemon(): #it gets the no of tasks running on the job
    global job_popens_live
    return_json={}
    return_json['d_ID']=str(d_ID)
    if job_popens_live=={}:
        return_json["NoTASKS"]=0
    else:
        return_json["NoTASKS"]=len(job_popens_live)
    return return_json
