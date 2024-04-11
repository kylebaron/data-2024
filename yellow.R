#' Download NYC Yellow Taxi 2015 Trip Data

library(vroom)
library(data.table)
library(readr)
library(microbenchmark)
library(qs)
library(arrow)
library(fst)
library(dplyr)
library(glue)
options(pillar.width = Inf, timeout = 300)

link <- "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month}.parquet"

months <- formatC(1:12, width = 2, flag = "0")

for(month in months) {
  this <- glue(link)
  dest <- file.path("yellow", basename(this))
  download.file(this, dest)
}

ds <- open_dataset("yellow")
data <- ds %>% filter(as_tibble(ds)
data <- mutate(data, tpep_pickup_datetime = as.character(tpep_pickup_datetime))
data <- mutate(data, tpep_pickup_datetime = parse_datetime(tpep_pickup_datetime))
data <- mutate(data, month = lubridate::month(tpep_pickup_datetime))
data <- mutate(data, day = lubridate::day(tpep_pickup_datetime))
group_by(data, month, day) %>% write_dataset("yellow-2023")
