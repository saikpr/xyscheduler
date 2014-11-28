#!/usr/bin/python
from xyvar import c_ID,master_ip,master_Port
from sys import argv
import httplib,json



if __name__=="__main__":
    del argv[0]
    tempjob={}
    for i in xrange(0,len(argv)):
        tempjob['ARG-'+str(i)]=argv[i]
    tempjob['c_ID']=c_ID
    tempjob['NumArgs']=len(argv)
    entity = json.dumps(str(tempjob))
    master_connect=httplib.HTTPConnection(master_ip,master_Port)
    master_connect.request("POST", "/addjob",entity)
    response=master_connect.getresponse()
    print response.status, response.reason
    print "Job ID: ",response.read()
    master_connect.close()
