this project will comprise of basically 2 basic daemons
1) The listener daemon (will run on Slaves). will listen on 8091 port. known as slave daemon.
2) The Job Scheduling Daemon (win run on master). will listen on 8090 port. known as master daemon
Along with this we will require a program to push the jobs

this project will also comprise of one  applications responsible
1) To add jobs
2) to delete jobs
3) Show status of jobs 
syntax : ./name.py {add,del,stat} {"arg-0":"  ", "arg-1": " "..... }

the daemons will communicate between them using http (using bottle)

master daemon will have multiple queues : job remaining, job running, job completed, machine status

once job is added it will first be given a index (say starting from md5 of time it started,epoch time) and will be added to the writeahead type log file(easier recovery).
once job is added it will then be added to a scheduler, queue in this case(implemented using python)
once a system is ready to run a job, it will added to another log file
once completed, it will be added to another log file
