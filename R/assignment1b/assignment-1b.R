## loading customer data
customer <- read.csv("customer.csv", header = TRUE)
head(customer)

str(customer)

## normalizing data of different numerical values, excluding first column (ID)
customer = scale(customer[, -1])

## clustering, Euclidian Distance
hc = hclust(dist(customer), method = "ward.D2")
hc

## cex: size of labels, hang: label positioning, negative value lets them hang down from 0
plot(hc, hang = -0.01, cex = 0.7)

hc2 = hclust(dist(customer), method = "single")
hc2
plot(hc2, hang = -0.01, cex = 0.7)

## differences between Ward.D2 and Single Method???

## cutting tree into clusters and visualizing
fit = cutree(hc, k = 4)
fit
table(fit)

plot(hc) 
rect.hclust(hc, k = 4, border = "red")

plot(hc) 
rect.hclust(hc, k = 4, which = 2, border = "red")

## PART 2 - K-MEANS CLUSTERING
set.seed(22)
fit = kmeans(customer, 4)
fit

plot(customer, col = fit)
plot(customer[, -c(1,2)], col = "red")

## PART 3 - COMPARING CLUSTERS
install.packages("fpc")
library(fpc)

## preparing three clusterings
single_c = hclust(dist(customer), method = "single")
hc_single = cutree(single_c, k = 4)

complete_c = hclust(dist(customer), method = "complete")
hc_complete = cutree(complete_c, k = 4)

set.seed(22)
km = kmeans(customer, 4)

## WSS / Silhouette
cs = cluster.stats(dist(customer), km$cluster)
cs[c("within.cluster.ss", "avg.silwidth")]

sapply(list(kmeans = km$cluster, 
            hc_single = hc_single,
            hc_complete = hc_complete), 
       function(c) cluster.stats(dist(customer), c)
       [c("within.cluster.ss","avg.silwidth")])

## trying a good k for kmeans
nk = 2:10
set.seed(22)

#WSS
WSS = sapply(nk, function(k) {kmeans(customer, centers = k) $tot.withinss})
WSS
##plot it
plot(nk, WSS, type="l", xlab= "number of k", ylab="within sum of
squares")

## silhouette
SW = sapply(nk, function(k) { cluster.stats(dist(customer),
                                            kmeans(customer, centers=k)$cluster)$avg.silwidth})
SW
# plot it
plot(nk, SW, type="l", xlab="number of clusters", ylab="average
     silhouette width")
