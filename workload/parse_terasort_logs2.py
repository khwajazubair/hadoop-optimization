!/usr/bin/python
import os, os.path
import csv
import time
import datetime
import sys
#Returns job ID
def jobID(fileName):

 for line in open(fileName):
  if "Running job" in line:
   p=line.split(" ")
   return p[6]




def jobStartTime(fileName):

 for line in open(fileName):
  if "Running job:" in line:
   p=line.split(" ")
   return p[1]

 return "0"


def jobCompletionTime(fileName):

 for line in open(fileName):
  if "completed successfully" in line:
   p=line.split(" ")
   return p[1]


 return "0"


#Returns 1 for successfull completion and 0 otherwise
def isJobSuccessful(fileName):

 for line in open(fileName):
  if "completed successfully" in line:
   return str(1)

 return "0"


#Returns number of map tasks launched
def mapTasks(fileName):

 for line in open(fileName):
  if "Launched map tasks" in line:
   p=line.split("=")
   return p[1]

 return "0"



#Returns number of reduce tasks launched
def reduceTasks(fileName):

 for line in open(fileName):
  if "Launched reduce tasks" in line:
   p=line.split("=")
   return p[1]


 return "0"



#Returns number of bytes read
def bytesRed(fileName):

 for line in open(fileName):
  if "HDFS: Number of bytes read" in line:
   p=line.split("=")
   return p[1]


 return "0"


#Returns number of byte written
def bytesWritten(fileName):

 for line in open(fileName):
  if "HDFS: Number of bytes written" in line:
   p=line.split("=")
   return p[1]


 return "0"


#Returns the total map time in mili seconds
def mapTime(fileName):

 for line in open(fileName):
  if "Total time spent by all maps" in line:
   p=line.split("=")
   return p[1]


 return "0"



#Returns total of reduce time in mili seconds
def reduceTime(fileName):

 for line in open(fileName):
  if "Total time spent by all reduces" in line:
   p=line.split("=")
   return p[1]


 return "a"


#Returns total time job took to complete. 
def jobTime(fileName, start, end):
   t1=time.strptime(end+",000".split(',')[0],'%H:%M:%S')
   t2=time.strptime(start+",000".split(',')[0],'%H:%M:%S')
   x=datetime.timedelta(hours=t1.tm_hour,minutes=t1.tm_min,seconds=t1.tm_sec).total_seconds()
   y=datetime.timedelta(hours=t2.tm_hour,minutes=t2.tm_min,seconds=t2.tm_sec).total_seconds()
   p=x-y
   return str(p)


#Returns CPU time in mili seconds for this task
def cpuTime(fileName):

 for line in open(fileName):
  if "CPU time spent" in line:
   p=line.split("=")
   return p[1]
 return "0"


#Returns memory amount in byts , used for this task
def memoryAmount(fileName):

 for line in open(fileName):
  if "Physical memory (bytes)" in line:
   p=line.split("=")
   return p[1]
 return "0"


#Returns number of failed shuffles
def failShuffle(fileName):
 for line in open(fileName):
  if "Failed Shuffles" in line:
   p=line.split("=")
   return p[1]
 return "0"


def killedTasks(fileName):
  count=0
  for line in open(fileName):

    if "Killed map tasks" in line:
      p=line.split("=")
      count=count+int(p[1])
    if "Killed reduce tasks" in line:
      p=line.split("=")
      count=count+int(p[1])
  return str(count)


#Main method of the program
def main(dirName):

 path, dirs, files = os.walk("/home/ubuntu/logs").next()
 #file_count = len(files)
 #print "total job files are %s"%file_count
 for i in range(5):
    c=str(i+1)
    fileName=dirName+"/run.out-"+c
  
    a=jobID(fileName)
    b=jobStartTime(fileName)
    c=jobCompletionTime(fileName)
    if c!="0" and b!="0":
         d=jobTime(fileName, b, c)
    else:
         d="0"
    e=mapTasks(fileName)
    f=reduceTasks(fileName)
    g=bytesRed(fileName)
    h=bytesWritten(fileName)
    i=mapTime(fileName)
    j=reduceTime(fileName)
    k=isJobSuccessful(fileName)
    l=cpuTime(fileName)
    m=memoryAmount(fileName)
    n=failShuffle(fileName)
    o=killedTasks(fileName)
    log=map(lambda x: x.strip(), [a,b,c,d,e,f,g,h,i,j,k,l,m,n,o])
    #log=([a,b,c,d,e,f,g,h,i,j,k,l,m,n,o])
    # print log
    with open('$dirName/jobs.out', 'ab') as csvout:
         csvout = csv.writer(csvout, delimiter = ' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL )
         csvout.writerow(log)
   


main(sys.argv[1])

