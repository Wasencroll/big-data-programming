## libraries used (install and load)
install.packages("plyr")
install.packages("ggplot2")
install.packages("reshape2")
install.packages()

## loading required libraries
library(plyr)
library(ggplot2)
library(reshape2)

## wd already set
getwd()

## loading vehicle data
vehicles <- read.csv(unz("vehicles.csv.zip", "vehicles.csv"),
                     stringsAsFactors = F)
## with factors, we can do something like below to see how many entries of values exist
table(vehicles$year)

## display 
head(vehicles)

labels <- do.call(rbind, strsplit(readLines("varlabels.txt"), " - "))
labels <- read.table("labels", sep = "-", header = FALSE)

head(labels)
nrow(vehicles)
ncol(vehicles)
names(vehicles)

length(unique(vehicles[, "year"])) ## comma wildcard for column position
length(unique(vehicles[["year"]])) ## find column by name

min(vehicles[, "year"])
max(vehicles[, "year"])

library(dplyr)
1984data <- vehicles %>%
  filter("year" == 1984)

vehicles[vehicles$year == 1984,]

## counting number of car models with automatic/manual gear box
vehicles$trany[vehicles$trany == ""] <- NA
vehicles$trany2 <- ifelse(substr(vehicles$trany, 1, 4) == "Auto", "Auto", "Manual")
vehicles$trany2 <- as.factor(vehicles$trany2)
table(vehicles$trany)
table(vehicles$trany2)

mpgByYr <- ddply(vehicles, 
                 ~year, 
                 summarise,
                 avgMPG = mean(comb08),
                 avgHghy = mean(highway08),
                 avgCity = mean(city08))
mpgByYr

##alternative using piping and dplyr
##library(dplyr)
##vehicles %>%
##  select(year, comb08, highway08, city08) %>%
##  group_by(year) %>%
##  summarize(avgMPG = mean(comb08),
##            avgHghy = mean(highway08),
##            avgCity = mean(city08))

## plotting mpgByYr Data
ggplot(mpgByYr, aes(year, avgMPG)) +
  geom_point() +
  geom_smooth() +
  xlab("Year") +
  ylab("Average MPG") +
  ggtitle("All cars")

## 24 mpg = 40 km/gallon = 40km / 3,8 L = 100 km / 9,5 L
## 20 mpg = 34 km/gallon = 34km / 3,8 L = 100 km / 11,4 L

## Has mix of cars sold changed (compare 1985-2000 with 2000-2015)
library(dplyr)
before_2000 <-vehicles %>%
  filter(year < 2000) %>%
  select(year, VClass, comb08) %>%
  group_by(VClass) %>%
  summarize(before2000 = n(),
            prev_avgMPG = mean(comb08))

since_2000 <- vehicles %>%
  filter(year >= 2000) %>%
  select(year, VClass, comb08) %>%
  group_by(VClass) %>%
  summarize(since2000 = n(),
            cur_avgMPG = mean(comb08))

diff <- before_2000 %>%
  select(VClass, before2000, prev_avgMPG) %>%
  full_join(since_2000, by = "VClass") %>%
  select(VClass, before2000, prev_avgMPG, since2000, cur_avgMPG) %>%
  group_by(VClass, before2000, prev_avgMPG, since2000, cur_avgMPG) %>%
  summarize(impr = sum(cur_avgMPG - prev_avgMPG)) %>%
  arrange(desc(impr))

## Demonstration 2
gasCars <- subset(vehicles, fuelType1 %in% c("Regular Gasoline", "Premium Gasoline", "Midgrade Gaoline") &
                  fuelType2 == "" & atvType != "Hybrid")

mpgGasCars <- ddply(gasCars, ~year, summarise,
                    avgMPG = mean(comb08))

ggplot(mpgGasCars, aes(year, avgMPG)) +
  geom_point() +
  geom_smooth() +
  xlab("Year") +
  ylab("Average MPG") +
  ggtitle("Gas Cars Avg MPG by Year")