


#install.packages("ggplot2")
require(ggplot2)
library(ggplot2)





exp1_bs = read.table("exp_1_bs.csv", header=TRUE)
df <- data.frame(exp1_bs)


df1 <- df[, c(4, 15, 16, 17,18)]

#cpt_r1 = df1[which(df1$r==1 & df1$sp=="t" & df1$s=="cp"), ] 

cpt <- matrix (ncol=5, byrow=TRUE)
cpf <- matrix (ncol=5, byrow=TRUE)
fst <- matrix (ncol=5, byrow=TRUE)
fsf <- matrix (ncol=5, byrow=TRUE)


#store all capcity true scheduler to 'cpt' matrix
for(i in 1:5){
	run = paste('r',i,sep="")
	
	run = df1[which(df1$r==i & df1$sp=="t" & df1$s=="cp"), ]
	run = run[, c(1)]
	cpt <- rbind(cpt,c(run))
}


#store all capcity false scheduler to 'cpf' matrix
for(i in 1:5){
	run = paste('r',i,sep="")
	
	run = df1[which(df1$r==i & df1$sp=="f" & df1$s=="cp"), ]
	run = run[, c(1)]
	cpf <- rbind(cpf,c(run))
}

#store all fairshare true scheduler to 'fst' matrix
for(i in 1:5){
	run = paste('r',i,sep="")
	
	run = df1[which(df1$r==i & df1$sp=="t" & df1$s=="fs"), ]
	run = run[, c(1)]
	fst <- rbind(fst,c(run))
}


#store all fairshare false scheduler to 'fsf' matrix
for(i in 1:5){
	run = paste('r',i,sep="")
	
	run = df1[which(df1$r==i & df1$sp=="f" & df1$s=="fs"), ]
	run = run[, c(1)]
	fsf <- rbind(fsf,c(run))
}

















#Drop 'NA' values from matrix
cpt <- cpt[!is.na(cpt[,1]),, drop = FALSE]
cpf <- cpf[!is.na(cpf[,1]),, drop = FALSE]
fst <- fst[!is.na(fst[,1]),, drop = FALSE]
fsf <- fsf[!is.na(fsf[,1]),, drop = FALSE]

#print matrices first row, column 1 to 5
#print(cpt[1,1:5])
#print(cpf[1,1:5])
#print(fst[1,1:5])
#print(fsf[1,1:5])

#bs_ct_1=read.csv("bs_ct_1.csv", header=FALSE)
#bs_ft_1=read.csv("bs_ft_1.csv", header=FALSE)

run_no=1

for ( i in 1:5){

	#declar variable names for each run
	cptx=paste("cpt",i,sep="")
	cpfx=paste("cpf",i,sep="")
	fstx=paste("fst",i,sep="")
	fsfx=paste("fsf",i,sep="")
    
    #read matrix and store in variable
	cptx=cpt[i,1:5]
	cpfx=cpf[i,1:5]
	fstx=fst[i,1:5]
	fsfx=fsf[i,1:5]

	if(i==run_no){

		#create matrix for ploting
		pl = matrix(ncol=5, byrow=TRUE)

		pl <- rbind(pl,c(cptx))
		pl <- rbind(pl,c(fstx))
		pl <- rbind(pl,c(cpfx))
		pl <- rbind(pl,c(fsfx))

		#cpt <- cpt[!is.na(pl[,1]),, drop = FALSE]


		#print(pl)
		#barplot(pl,beside=TRUE,xlab="experiment set", ylab="job completion time(sec)",xlim=c(0,30),main="Baseline Bar plot")

		#print("cpt")
		#print(summary(cptx))
		#print("fst")
		#print(summary(fstx))

		#print("cpf")
		#print(summary(cpfx))
		#print("fsf")
		#print(summary(fsfx))
	}

}



print(summary(cpt))


#counts <- table(cpt, fst)
#barplot(counts, main="Cpt vs Fst",
#  xlab="Number of Exp", col=c("darkblue","red"),
# 	 legend = colnames(counts), beside=TRUE)

#plot(cpt1)
#hist(cpt,xlab="Job Completion Time in Seconds", ylab="Count")

#barplot(cpt, main="Capacity vs Fairshar (Default)",
# xlab="Number of experiments", col=c("red","blue"))
#qplot(cpt1,fst1)

#require(ggplot2)
#data(diamonds)
#head(diamonds)
#ggplot(diamonds, aes(clarity, fill=cut)) + geom_bar(position="dodge") +
#opts(title="Examplary Grouped Barplot")

#min,max,mean values for capacity scheduler, spec = true
#min_ct=min(capacity_t$V1)
#max_ct=max(capacity_t$V1)
#mean_ct=sapply(capacity_t$V1, mean)


#print(max_ff)
#baseline_exp =c(capacity_t$V1,capacity_f$V1,fair_t$V1,fair_f$V1)
#run_1 <- table(bs_ct_1$V1,bs_ft_1$V1)
#barplot(run_1,main="Capacity vs Fair Default",
 # xlab="Job Completion in Seconds", col=c("darkblue","red"),
# 	 legend = rownames(run_1),beside=TRUE)
