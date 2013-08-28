


#install.packages("ggmeanPl1ot2")
#require(ggmeanPl1ot2)
#library(ggmeanPl1ot2)




#Data Table of each experiment case 
exp1_bs = read.table("../exp_1/exp_1_bs.csv", header=TRUE)
exp1_co_dn = read.table("../exp_1/exp_1_bs_new.csv", header=TRUE)
#exp1_co_cl = read.table("exp-1/exp_1_co_cl.csv", header=TRUE)


#Dataframe for each experiment case
bs1_Df <- data.frame(exp1_bs)
dn1_Df <- data.frame(exp1_co_dn)
#cl1_Df <- data.frame(exp1_co_cl)



#Sub data frame for each case
bsDf1 <- bs1_Df[, c(4, 12, 13, 15, 16, 17, 18 )]
dnDf1 <- dn1_Df[, c(4, 12, 13, 15, 16, 17, 18 )]
#clDf1 <- cl1_Df[, c(4, 15, 16, 17,18)]






###########################################################
#####          Functions  Section                  #######
###########################################################


matrixCreator <- function(df,spec,schedule,total_run, col){

	matrixC <- matrix (ncol=5, byrow=TRUE)

	for(i in 1:total_run){
	run = paste('r',i,sep="")
	
	run = df[which(df$r==i & df$sp==spec & df$s==schedule), ]
	run = run[, c(col)]
	matrixC <- rbind(matrixC,c(run))
	}

	matrixC <- matrixC[!is.na(matrixC[,1]),, drop = FALSE]
	return(matrixC)
}




#Sum of JobTimes
sumOf <- function(matrix1){
   
   		x1 = sum(matrix1[1,1:5])
		x2 = sum(matrix1[2,1:5])
		x3 = sum(matrix1[3,1:5])
		x4 = sum(matrix1[4,1:5])
		x5 = sum(matrix1[5,1:5]) 
		x = c(x1, x2, x3, x4, x5)
 	return(x)
}




#Mean of matrix
meanOf <- function(matrix1){
    
		x1 = mean(matrix1[1,1:5])
		x2 = mean(matrix1[2,1:5])
		x3 = mean(matrix1[3,1:5])
		x4 = mean(matrix1[4,1:5])
		x5 = mean(matrix1[5,1:5]) 
		x = c(x1, x2, x3, x4, x5)
		#print(x)
		
 	return (x)
}


# Print Section
printSum <- function(sum1) {

	if (sum1==1){

		#sum of Job Duration 
		print("bs sum of job time for cpt")
		print(bs_sum_cpt)
		print("bs sum of job time for cpf")
		print(bs_sum_cpf)
		print("bs sum of job time for fst")
		print(bs_sum_fst)
		print("bs sum of job time for fsf")
		print(bs_sum_fsf)
		
		print("dn sum of jobs for cpt")
		print(dn_sum_cpt)
		print("dn sum of jobs for cpf")
		print(dn_sum_cpt)
		print("dn sum of jobs for fst")
		print(dn_sum_fst)
		print("dn sum of jobs for fsf")
		print(dn_sum_fsf)
		


		#sum of killed tasks
		print("bs_sum_cpt_tasks")
		print(bs_sum_cpt_tasks)
		print("bs_fst_tasks")
		print(bs_sum_fst_tasks)

		print("dn_cpt_tasks")
		print(dn_sum_cpt_tasks)
		print("dn_fst_tasks")
		print(dn_sum_fst_tasks)

		#sum of Memory amount
		print("bs sum of mem_amount for cpt")
		print(bs_sum_cpt_mem)
		print("bs sum of mem_amount for cpf")
		print(bs_sum_cpf_mem)
		print("bs sum of mem_amount for fst")
		print(bs_sum_fst_mem)
		print("bs sum of mem_amount for fsf")
		print(bs_sum_fsf_mem)

		print("dn sum of mem_amount for cpt")
		print(dn_sum_cpt_mem)
		print("dn sum of mem_amount for cpf")
		print(dn_sum_cpt_mem)
		print("dn sum of mem_amount for fst")
		print(dn_sum_fst_mem)
		print("dn sum of mem_amount for fsf")
		print(dn_sum_fsf_mem)

		#sum of CPU Time
		print("bs sum of cpu_t for cpt")
		print(bs_sum_cpt_cpu)
		print("bs sum of cpu_t for cpf")
		print(bs_sum_cpf_cpu)
		print("bs sum of cpu_t for fst")
		print(bs_sum_fst_cpu)
		print("bs sum of cpu_t for fsf")
		print(bs_sum_fsf_cpu)

		print("dn sum of cpu_t for cpt")
		print(dn_sum_cpt_cpu)
		print("dn sum of cpu_t for cpf")
		print(dn_sum_cpt_cpu)
		print("dn sum of cpu_t for fst")
		print(dn_sum_fst_cpu)
		print("dn sum of cpu_t for fsf")
		print(dn_sum_fsf_cpu)


	}
	
	if(sum1!=1)
	print("No sum to print, change sum1 variable to print something!")
}




printMean <- function(mean1){

	if (mean1==1){

		#mean of Job Duration 
		print("bs mean of job time for cpt")
		print(bs_mean_cpt)
		print("bs mean of job time for cpf")
		print(bs_mean_cpf)
		print("bs mean of job time for fst")
		print(bs_mean_fst)
		print("bs mean of job time for fsf")
		print(bs_mean_fsf)
		
		print("dn mean of jobs for cpt")
		print(dn_mean_cpt)
		print("dn mean of jobs for cpf")
		print(dn_mean_cpt)
		print("dn mean of jobs for fst")
		print(dn_mean_fst)
		print("dn mean of jobs for fsf")
		print(dn_mean_fsf)
		


		#mean of killed tasks
		print("bs_mean_cpt_tasks")
		print(bs_mean_cpt_tasks)
		print("bs_fst_tasks")
		print(bs_mean_fst_tasks)

		print("dn_cpt_tasks")
		print(dn_mean_cpt_tasks)
		print("dn_fst_tasks")
		print(dn_mean_fst_tasks)

		#mean of Memory amount
		print("bs mean of mem_amount for cpt")
		print(bs_mean_cpt_mem)
		print("bs mean of mem_amount for cpf")
		print(bs_mean_cpf_mem)
		print("bs mean of mem_amount for fst")
		print(bs_mean_fst_mem)
		print("bs mean of mem_amount for fsf")
		print(bs_mean_fsf_mem)

		print("dn mean of mem_amount for cpt")
		print(dn_mean_cpt_mem)
		print("dn mean of mem_amount for cpf")
		print(dn_mean_cpt_mem)
		print("dn mean of mem_amount for fst")
		print(dn_mean_fst_mem)
		print("dn mean of mem_amount for fsf")
		print(dn_mean_fsf_mem)

		#mean of CPU Time
		print("bs mean of cpu_t for cpt")
		print(bs_mean_cpt_cpu)
		print("bs mean of cpu_t for cpf")
		print(bs_mean_cpf_cpu)
		print("bs mean of cpu_t for fst")
		print(bs_mean_fst_cpu)
		print("bs mean of cpu_t for fsf")
		print(bs_mean_fsf_cpu)

		print("dn mean of cpu_t for cpt")
		print(dn_mean_cpt_cpu)
		print("dn mean of cpu_t for cpf")
		print(dn_mean_cpt_cpu)
		print("dn mean of cpu_t for fst")
		print(dn_mean_fst_cpu)
		print("dn mean of cpu_t for fsf")
		print(dn_mean_fsf_cpu)


	}
	if(mean1!=1)
		print("No mean, change mean1 to print something")

}

printGmean <- function(mean1){

	if (mean1==1){

		#mean of Job Duration 
		print("bs G_mean of job time for cpt")
		print(mean(bs_mean_cpt))
		print("bs Gmean of job time for cpf")
		print(mean(bs_mean_cpf))
		print("bs Gmean of job time for fst")
		print(mean(bs_mean_fst))
		print("bs Gmean of job time for fsf")
		print(mean(bs_mean_fsf))
		
		print("dn Gmean of jobs for cpt")
		print(mean(dn_mean_cpt))
		print("dn Gmean of jobs for cpf")
		print(mean(dn_mean_cpf))
		print("dn Gmean of jobs for fst")
		print(mean(dn_mean_fst))
		print("dn Gmean of jobs for fsf")
		print(mean(dn_mean_fsf))
		


		#Gmean of killed tasks
		print("bs_mean_cpt_tasks")
		print(mean(bs_mean_cpt_tasks))
		print("bs_fst_tasks")
		print(mean(bs_mean_fst_tasks))

		print("dn_cpt_tasks")
		print(mean(dn_mean_cpt_tasks))
		print("dn_fst_tasks")
		print(mean(dn_mean_fst_tasks))

		#Gmean of Memory amount
		print("bs Gmean of mem_amount for cpt")
		print(mean(bs_mean_cpt_mem))
		print("bs Gmean of mem_amount for cpf")
		print(mean(bs_mean_cpf_mem))
		print("bs Gmean of mem_amount for fst")
		print(mean(bs_mean_fst_mem))
		print("bs Gmean of mem_amount for fsf")
		print(mean(bs_mean_fsf_mem))

		print("dn Gmean of mem_amount for cpt")
		print(mean(dn_mean_cpt_mem))
		print("dn Gmean of mem_amount for cpf")
		print(mean(dn_mean_cpt_mem))
		print("dn Gmean of mem_amount for fst")
		print(mean(dn_mean_fst_mem))
		print("dn Gmean of mem_amount for fsf")
		print(mean(dn_mean_fsf_mem))

		#Gmean of CPU Time
		print("bs Gmean of cpu_t for cpt")
		print(mean(bs_mean_cpt_cpu))
		print("bs Gmean of cpu_t for cpf")
		print(mean(bs_mean_cpf_cpu))
		print("bs Gmean of cpu_t for fst")
		print(mean(bs_mean_fst_cpu))
		print("bs Gmean of cpu_t for fsf")
		print(mean(bs_mean_fsf_cpu))

		print("dn Gmean of cpu_t for cpt")
		print(mean(dn_mean_cpt_cpu))
		print("dn Gmean of cpu_t for cpf")
		print(mean(dn_mean_cpt_cpu))
		print("dn GGmean of cpu_t for fst")
		print(mean(dn_mean_fst_cpu))
		print("dn GGmean of cpu_t for fsf")
		print(mean(dn_mean_fsf_cpu))


	}
	if(mean1!=1)
		print("No mean, change mean1 to print something")

}


########################################################
#Column variabels to Fetch data from Df1 and total run
job_d=1
cpu_t=2
mem_amount=3
k_tasks=4
run_total = 25
sum1 =0 
mean1=1

###########################################################
#####          Baseline Section           #######
###########################################################

#baseline dataframe
Df1 = bsDf1

######################################
#Matrices from Colocated Datanode data

bs_cpt <- matrixCreator(Df1,"t","cp",run_total,job_d)
bs_cpf <- matrixCreator(Df1,"f","cp",run_total,job_d)
bs_fst <- matrixCreator(Df1,"t","fs",run_total,job_d)
bs_fsf <- matrixCreator(Df1,"f","fs",run_total,job_d)

##### Killed Tasks ###########
bs_cpt_tasks <- matrixCreator(Df1,"t","cp",run_total,k_tasks)
bs_cpf_tasks <- matrixCreator(Df1,"f","cp",run_total,k_tasks)
bs_fst_tasks <- matrixCreator(Df1,"t","fs",run_total,k_tasks)
bs_fsf_tasks <- matrixCreator(Df1,"f","fs",run_total,k_tasks)

########## Memory amount #######
bs_cpt_mem <- matrixCreator(Df1,"t","cp",run_total,mem_amount)
bs_cpf_mem <- matrixCreator(Df1,"f","cp",run_total,mem_amount)
bs_fst_mem <- matrixCreator(Df1,"t","fs",run_total,mem_amount)
bs_fsf_mem <- matrixCreator(Df1,"f","fs",run_total,mem_amount)

######### CPU Time ################
bs_cpt_cpu <- matrixCreator(Df1,"t","cp",run_total,cpu_t)
bs_cpf_cpu <- matrixCreator(Df1,"f","cp",run_total,cpu_t)
bs_fst_cpu <- matrixCreator(Df1,"t","fs",run_total,cpu_t)
bs_fsf_cpu <- matrixCreator(Df1,"f","fs",run_total,cpu_t)



#################################
#bs Sum Section 

bs_sum_cpt <- sumOf(bs_cpt)
bs_sum_cpf <- sumOf(bs_cpf)
bs_sum_fst <- sumOf(bs_fst)
bs_sum_fsf <- sumOf(bs_fsf)

bs_sum_cpt_mem <- sumOf(bs_cpt_mem)
bs_sum_cpf_mem <- sumOf(bs_cpf_mem)
bs_sum_fst_mem <- sumOf(bs_fst_mem)
bs_sum_fsf_mem <- sumOf(bs_fsf_mem)

bs_sum_cpt_cpu <- sumOf(bs_cpt_cpu)
bs_sum_cpf_cpu <- sumOf(bs_cpf_cpu)
bs_sum_fst_cpu <- sumOf(bs_fst_cpu)
bs_sum_fsf_cpu <- sumOf(bs_fsf_cpu)

bs_sum_cpt_tasks <- sumOf(bs_cpt_tasks)
bs_sum_cpf_tasks <- sumOf(bs_cpf_tasks)
bs_sum_fst_tasks <- sumOf(bs_fst_tasks)
bs_sum_fsf_tasks <- sumOf(bs_fsf_tasks)



##################################
#bs Mean Section

bs_mean_cpt <- meanOf(bs_cpt)
bs_mean_cpf <- meanOf(bs_cpf)
bs_mean_fst <- meanOf(bs_fst)
bs_mean_fsf <- meanOf(bs_fsf)

bs_mean_cpt_mem <- meanOf(bs_cpt_mem)
bs_mean_cpf_mem <- meanOf(bs_cpf_mem)
bs_mean_fst_mem <- meanOf(bs_fst_mem)
bs_mean_fsf_mem <- meanOf(bs_fsf_mem)

bs_mean_cpt_cpu <- meanOf(bs_cpt_cpu)
bs_mean_cpf_cpu <- meanOf(bs_cpf_cpu)
bs_mean_fst_cpu <- meanOf(bs_fst_cpu)
bs_mean_fsf_cpu <- meanOf(bs_fsf_cpu)

bs_mean_cpt_tasks <- meanOf(bs_cpt_tasks)
bs_mean_cpf_tasks <- meanOf(bs_cpf_tasks)
bs_mean_fst_tasks <- meanOf(bs_fst_tasks)
bs_mean_fsf_tasks <- meanOf(bs_fsf_tasks)






###########################################################
#####          Colocated Datanode Section           #######
###########################################################

#colocated dn data frame
Df1 = dnDf1

#Matrices from Colocated Datanode data
dn_cpt <- matrixCreator(Df1,"t","cp",run_total,job_d)
dn_cpf <- matrixCreator(Df1,"f","cp",run_total,job_d)
dn_fst <- matrixCreator(Df1,"t","fs",run_total,job_d)
dn_fsf <- matrixCreator(Df1,"f","fs",run_total,job_d)

########### Killed Tasks #############
dn_cpt_tasks <- matrixCreator(Df1,"t","cp",run_total,k_tasks)
dn_cpf_tasks <- matrixCreator(Df1,"f","cp",run_total,k_tasks)
dn_fst_tasks <- matrixCreator(Df1,"t","fs",run_total,k_tasks)
dn_fsf_tasks <- matrixCreator(Df1,"f","fs",run_total,k_tasks)

############ Memory Amount #############
dn_cpt_mem <- matrixCreator(Df1,"t","cp",run_total,mem_amount)
dn_cpf_mem <- matrixCreator(Df1,"f","cp",run_total,mem_amount)
dn_fst_mem <- matrixCreator(Df1,"t","fs",run_total,mem_amount)
dn_fsf_mem <- matrixCreator(Df1,"f","fs",run_total,mem_amount)

############# CPU Time #################
dn_cpt_cpu <- matrixCreator(Df1,"t","cp",run_total,cpu_t)
dn_cpf_cpu <- matrixCreator(Df1,"f","cp",run_total,cpu_t)
dn_fst_cpu <- matrixCreator(Df1,"t","fs",run_total,cpu_t)
dn_fsf_cpu <- matrixCreator(Df1,"f","fs",run_total,cpu_t)

####################################
# Sum Section for Colocated Datanodes
dn_sum_cpt <- sumOf(dn_cpt)
dn_sum_cpf <- sumOf(dn_cpf)
dn_sum_fst <- sumOf(dn_fst)
dn_sum_fsf <- sumOf(dn_fsf)

dn_sum_cpt_mem <- sumOf(dn_cpt_mem)
dn_sum_cpf_mem <- sumOf(dn_cpf_mem)
dn_sum_fst_mem <- sumOf(dn_fst_mem)
dn_sum_fsf_mem <- sumOf(dn_fsf_mem)

dn_sum_cpt_cpu <- sumOf(dn_cpt_cpu)
dn_sum_cpf_cpu <- sumOf(dn_cpf_cpu)
dn_sum_fst_cpu <- sumOf(dn_fst_cpu)
dn_sum_fsf_cpu <- sumOf(dn_fsf_cpu)

dn_sum_cpt_tasks <- sumOf(dn_cpt_tasks)
dn_sum_cpf_tasks <- sumOf(dn_cpf_tasks)
dn_sum_fst_tasks <- sumOf(dn_fst_tasks)
dn_sum_fsf_tasks <- sumOf(dn_fsf_tasks)



######################################
#Mean Section for Colocated Datanodes
dn_mean_cpt <- meanOf(dn_cpt)
dn_mean_cpf <- meanOf(dn_cpf)
dn_mean_fst <- meanOf(dn_fst)
dn_mean_fsf <- meanOf(dn_fsf)

dn_mean_cpt_mem <- meanOf(dn_cpt_mem)
dn_mean_cpf_mem <- meanOf(dn_cpf_mem)
dn_mean_fst_mem <- meanOf(dn_fst_mem)
dn_mean_fsf_mem <- meanOf(dn_fsf_mem)

dn_mean_cpt_cpu <- meanOf(dn_cpt_cpu)
dn_mean_cpf_cpu <- meanOf(dn_cpf_cpu)
dn_mean_fst_cpu <- meanOf(dn_fst_cpu)
dn_mean_fsf_cpu <- meanOf(dn_fsf_cpu)

dn_mean_cpt_tasks <- meanOf(dn_cpt_tasks)
dn_mean_cpf_tasks <- meanOf(dn_cpf_tasks)
dn_mean_fst_tasks <- meanOf(dn_fst_tasks)
dn_mean_fsf_tasks <- meanOf(dn_fsf_tasks)














#print(bs_cpt)
#printSum(1)
#printMean(0)
#printGsum(1)
#printGmean(mean1)

#print(mean(bs_cpt[1,1:5]))

# print(bs_mean_cpt)
printbsRuns <- function(set){

	print("----- cpt -------")
for(i in 1:5){

	print(c(mean(bs_cpt[i,1:5]), max(bs_cpt[i,1:5]) - (min(bs_cpt[i,1:5])), sd((bs_cpt[i,1:5])) ))
	

	# print(range(bs_cpt[1,1:5]))
	# print((sd(bs_cpt[1,1:5]))
}
# print(range(bs_cpt[1,1:5]))
# print(sd(bs_cpt[1,1:5]))
print("----- cpf -------")
for(i in 1:5){
	print(c(mean(bs_cpf[i,1:5]), max(bs_cpf[i,1:5]) - min(bs_cpf[i,1:5]), sd((bs_cpf[i,1:5])) ))
	

	# print(range(bs_cpt[1,1:5]))
	# print((sd(bs_cpt[1,1:5]))
}

print("----- fst -------")
for(i in 1:5){
	print(c(mean(bs_fst[i,1:5]), max(bs_fst[i,1:5]) - min(bs_fst[i,1:5]), sd((bs_fst[i,1:5])) ))
	

	# print(range(bs_cpt[1,1:5]))
	# print((sd(bs_cpt[1,1:5]))
}

print("----- fsf -------")
for(i in 1:5){
	print(c(mean(bs_fsf[i,1:5]), max(bs_fsf[i,1:5]) - min(bs_fsf[i,1:5]), sd((bs_fsf[i,1:5])) ))
	

	# print(range(bs_cpt[1,1:5]))
	# print((sd(bs_cpt[1,1:5]))
}







}


#Drop 'NA' values from matrix


#print matrices first row, column 1 to 5
#print(cpt[1,1:5])
#print(cpf[1,1:5])
#print(fst[1,1:5])
#print(fsf[1,1:5])



#create matrix for meanPl1oting
#meanPl1 = matrix(ncol=5, byrow=TRUE)








#for (i in 1:5){



 

	

	meanPl1 <- matrix (ncol=5, byrow=TRUE)		
	meanPl1 <- rbind(meanPl1,c(bs_mean_cpt))
	meanPl1 <- rbind(meanPl1,c(bs_mean_cpf))
	meanPl1 <- rbind(meanPl1,c(bs_mean_fst))
	meanPl1 <- rbind(meanPl1,c(bs_mean_fsf))
	meanPl1 <- meanPl1[!is.na(meanPl1[,1]),, drop = FALSE]
	#barplot(meanPl1,beside=TRUE,xlab="experiment set", ylab="job commeanPl1etion time(sec)",xlim=c(0,30),main="Mean of cpt,cpf,fst,fsf")
	#print(meanPl1)


	#meanPl2 <- matrix (ncol=4, byrow=TRUE)		
	#meanPl2 <- rbind(meanPl2,c(mean(dn_mean_cpt)))
	#meanPl2 <- rbind(meanPl2,c(mean(dn_mean_cpf)))
	#meanPl2 <- rbind(meanPl2,c(mean(dn_mean_fst)))
	#meanPl2 <- rbind(meanPl2,c(mean(dn_mean_fsf)))
	#meanPl2 <- meanPl1[!is.na(meanPl2[,1]),, drop = FALSE]
	#barplot(meanPl2,beside=TRUE,xlab="experiment set", ylab="job commeanPl1etion time(sec)",xlim=c(0,30),main="Mean of dn cpt,cpf,fst,fsf")

prnt2Func <- function(x){

	if (x==1){
	print("---Mean comparison")
	print("---cpt comp----")
	print(mean(bs_sum_cpt))
	print(mean(dn_sum_cpt))

	print("---cpf comp----")
	print(mean(bs_sum_cpf))
	print(mean(dn_mean_cpf))

	print("---fst comp----")
	print(mean(bs_mean_fst))
	print(mean(dn_mean_fst))

	print("---fsf comp----")
	print(mean(bs_mean_fsf))
	print(mean(dn_mean_fsf))

   }


}




printdnRuns <- function( ){

	print("----- cpt -------")
for(i in 1:5){

	print(c(sum(dn_cpt_tasks[i,1:5]),mean(dn_cpt_tasks[i,1:5]), max(dn_cpt_tasks[i,1:5]) - (min(dn_cpt_tasks[i,1:5])),
	 range(dn_cpt_tasks[i,1:5]),sd((dn_cpt_tasks[i,1:5])) ))
	

	# print(range(dn_cpt[1,1:5]))
	# print((sd(dn_cpt[1,1:5]))
}
# print(range(dn_cpt[1,1:5]))
# print(sd(dn_cpt[1,1:5]))
print("----- cpf -------")
for(i in 1:5){
	print(c(sum(dn_cpf_tasks[i,1:5]),mean(dn_cpf_tasks[i,1:5]), max(dn_cpf_tasks[i,1:5]) - (min(dn_cpf_tasks[i,1:5])),
	 range(dn_cpf_tasks[i,1:5]),sd((dn_cpf_tasks[i,1:5])) ))
	

	# print(range(dn_cpt[1,1:5]))
	# print((sd(dn_cpt[1,1:5]))
}

print("----- fst -------")
for(i in 1:5){
	print(c(sum(dn_fst_tasks[i,1:5]),mean(dn_fst_tasks[i,1:5]), max(dn_fst_tasks[i,1:5]) - (min(dn_fst_tasks[i,1:5])), 
		range(dn_fst_tasks[i,1:5]), sd((dn_fst_tasks[i,1:5])) ))
	

	# print(range(dn_cpt[1,1:5]))
	# print((sd(dn_cpt[1,1:5]))
}

print("----- fsf -------")
for(i in 1:5){
	print(c(sum(dn_fsf_tasks[i,1:5]),mean(dn_fsf_tasks[i,1:5]), max(dn_fsf_tasks[i,1:5]) - (min(dn_fsf_tasks[i,1:5])),
	 range(dn_fsf_tasks[i,1:5]), sd((dn_fsf_tasks[i,1:5])) ))
	

	# print(range(dn_cpt[1,1:5]))
	# print((sd(dn_cpt[1,1:5]))
	}
}

printdnRuns()   
