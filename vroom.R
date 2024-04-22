library(readr)
library(vroom)
library(data.table)
library(arrow)
library(lobstr)
library(dplyr)
options(pillar.width = Inf)

file <- "data/taxi.csv"

system.time(df <- read_csv(file))
lobstr::obj_size(df)

system.time(v <- vroom(file))
lobstr::obj_size(v)

names(v)
lobstr::obj_size(v)

unique(v$year)
lobstr::obj_size(v)

head(v)
lobstr::obj_size(v)

tail(v)
lobstr::obj_size(v)

v %>% group_by(vendor_name, passenger_count, payment_type) %>% 
  summarise(tip = mean(tip_amount))
lobstr::obj_size(v)

dt <- as.data.table(v)
lobstr::obj_size(dt)
lobstr::obj_size(df)

system.time(v1 <- vroom(file))
system.time(v2 <- vroom(file, altrep = FALSE))
system.time(v3 <- vroom(file, altrep = FALSE, col_select = c(1,3,5)))

lobstr::obj_size(v1)
lobstr::obj_size(v2)
lobstr::obj_size(v3)


library(mmap)
x <- rnorm(100)
x <- as.mmap(mtcars)
list.files(tempdir())
foo <- x[]
