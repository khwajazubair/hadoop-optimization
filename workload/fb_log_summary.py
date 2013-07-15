#!/usr/bin/python
import os, os.path
import csv

#Returns job ID
def jobID(fileName):

 for line in open(fileName):
  if "Running job" in line:
   p=line.split("_")
   return p[1]


def shuffleInputRatio(fileName):

 for line in open(fileName):
  if "shuffleInputRatio" in line:
   p=line.split("=")
   return p[1]

def outputShuffleRatio(fileName):

 for line in open(fileName):
  if "outputShuffleRatio" in line:
   p=line.split("=")
   return p[1]

#Returns 1 for successfull completion and 0 otherwise
def jobCompletion(fileName):

 for line in open(fileName):
  if "completed successfully" in line:
   return str(1)
  else:
   return str(0)

#Returns number of map tasks launched
def mapTasks(fileName):

 for line in open(fileName):
  if "Launched map tasks" in line:
   p=line.split("=")
   return p[1]

#Returns number of reduce tasks launched
def reduceTasks(fileName):

 for line in open(fileName):
  if "Launched reduce tasks" in line:
   p=line.split("=")
   return p[1]


#Returns number of bytes read
def bytesRed(fileName):

 for line in open(fileName):
  if "HDFS: Number of bytes read" in line:
   p=line.split("=")
   return p[1]

#Returns number of byte written
def bytesWritten(fileName):

 for line in open(fileName):
  if "HDFS: Number of bytes written" in line:
   p=line.split("=")
   return p[1]

#Returns the total map time in mili seconds
def mapTime(fileName):

 for line in open(fileName):
  if "Total time spent by all maps" in line:
   p=line.split("=")
   return p[1]

#Returns total of reduce time in mili seconds
def reduceTime(fileName):

 for line in open(fileName):
  if "Total time spent by all reduces" in line:
   p=line.split("=")
   return p[1]

#Returns total time job took to complete. 
def jobTime(fileName):

 for line in open(fileName):
  if "The job took" in line:
   p=line.split()
   return p[3]

#Returns CPU time in mili seconds for this task
def cpuTime(fileName):

 for line in open(fileName):
  if "CPU time spent" in line:
   p=line.split("=")
   return p[1]

#Returns memory amount in byts , used for this task
def memoryAmount(fileName):

 for line in open(fileName):
  if "Physical memory (bytes)" in line:
   p=line.split("=")
   return p[1]

#Returns number of failed shuffles
def failShuffle(fileName):
 for line in open(fileName):
  if "Failed Shuffles" in line:
   p=line.split("=")
   return p[1]

#Main method of the program
def main():

 path, dirs, files = os.walk("/home/ubuntu/logs").next()
 file_count = len(files)
 print "total job files are %s"%file_count
 for i in range(file_count):
    c=str(i)
    fileName="/home/ubuntu/logs/job-"+c+".txt"
    print "processing logs of job-"+c+".txt"
    a=jobID(fileName)
    b=shuffleInputRatio(fileName)
    c=outputShuffleRatio(fileName)
    d=jobCompletion(fileName)
    e=mapTasks(fileName)
    f=reduceTasks(fileName)
    g=bytesRed(fileName)
    h=bytesWritten(fileName)
    i=mapTime(fileName)
    j=reduceTime(fileName)
    k=jobTime(fileName)
    l=cpuTime(fileName)
    m=memoryAmount(fileName)
    n=failShuffle(fileName)
    log=map(lambda x: x.strip(), [a,b,c,d,e,f,g,h,i,j,k,l,m,n])
   # print log
    with open('/home/ubuntu/hadoop-3.0.0-SNAPSHOT/logs/facebook_log.csv', 'ab') as csvout:
         csvout = csv.writer(csvout, delimiter = ' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL )
         csvout.writerow(log)
    #for index in range(len(log)):
    # fbTrace.write(log[index])


main()
^_^_

