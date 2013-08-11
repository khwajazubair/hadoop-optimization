
library(ggplot2)




# A comment: this is a sample script.
y=c(200,400,600,800,1000,1200,1400)
x=c(1,2,3,4,5,6,7)
mean(y)
mean(x)
#plot(x,y)

 #library(plyr) 
 
 #listOfFiles <- list.files(pattern= ".txt") 
 
 #d <- ldply(listOfFiles, read.table) 
 #print(d)
capacity_t = read.table("baseline-capacity-spec-true.txt")
capacity_f = read.table("baseline-capacity-spec-false.txt")
fair_t = read.table("baseline-fair-spec-true.txt")
fair_f = read.table("baseline-fair-spec-false.txt")

bs_ct_1=read.csv("bs_ct_1.csv", header=FALSE)
bs_ft_1=read.csv("bs_ft_1.csv", header=FALSE)


#min,max,mean values for capacity scheduler, spec = true
min_ct=min(capacity_t$V1)
max_ct=max(capacity_t$V1)
mean_ct=sapply(capacity_t$V1, mean)

#min,max,mean values for capacity scheduler, spec = false
min_cf=min(capacity_f$V1)
max_cf=max(capacity_f$V1)
mean_cf=sapply(capacity_f$V1, mean)


#min,max,mean values for fair scheduler, spec = true
min_ft=min(fair_t$V1)
max_ft=max(fair_t$V1)
mean_ft=sapply(fair_t$V1,mean)

#mins,max,mean values for fair scheduler, spec = false
min_ff=min(fair_f$V1)
max_ff=max(fair_f$V1)
mean_ff=sapply(fair_f$V1, mean)
#barplot(table(capacity_t$V1))
#print(max_ff)
baseline_exp =c(capacity_t$V1,capacity_f$V1,fair_t$V1,fair_f$V1)
run_1 <- table(bs_ct_1$V1,bs_ft_1$V1)
barplot(run_1,main="Capacity vs Fair Default",
  xlab="Job Completion in Seconds", col=c("darkblue","red"),
 	 legend = rownames(run_1),beside=TRUE)

#b <- factor(c(bs_ct_1))
#summary(b)

#barplot(c(bs_ct_1$V1))
#print(bs_ct_1)
#print(summary(capacity_t))
#print(summary(capacity_f))
#qplot(c(bs_ct_1),c(bs_ft_1))

#hist(baseline_exp, breaks=4)#c(max_ct,max_cf,max_ft,max_ff), col="lightblue", xlab="Baseline Experiment", ylab="Count", main="Baseline Expirement")

#plot(x,y, type="hist")
#plot(x, y)


#p <- qplot(capacity_t$V1, xlab=names("capacity_t")
#do.call(grid.arrange, p)