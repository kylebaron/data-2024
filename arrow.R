library(arrow)
library(dplyr)
library(lobstr)
library(jsonlite)
library(microbenchmark)
options(pillar.width = Inf)

x <- read_csv_arrow(
  "data/taxi.csv", 
  as_data_frame = FALSE
)

y <- read_csv_arrow(
  "data/taxi.csv"
)

microbenchmark(
  x %>% 
    group_by(vendor_name, passenger_count) %>% 
    summarise(
      Mean = mean(total_amount), 
      Sd = sd(total_amount), 
      Min = min(total_amount), 
      Max = max(total_amount), 
      .groups = "drop"
    ) %>% collect()
)

microbenchmark(
  y %>% 
    group_by(vendor_name, passenger_count) %>% 
    summarise(
      Mean = mean(total_amount), 
      Sd = sd(total_amount), 
      Min = min(total_amount), 
      Max = max(total_amount), 
      .groups = "drop"
    ) 
)


microbenchmark(
  x %>% count(vendor_name, passenger_count)
)

microbenchmark(
  y %>% count(vendor_name, passenger_count)
)

y <- open_dataset("yellow")
nrow(y)

system.time(y %>% count(VendorID) %>% collect())
system.time(yy %>% count(VendorID) %>% collect())

write_dataset(
  y, 
  path = "yellow-csv",
  format = "csv", 
  partition = c("VendorID", "payment_type")
  
)

xx <- open_dataset("yellow-csv", format = "csv")

system.time(count(xx, VendorID) %>% collect())


lobstr::obj_size(x)


tax <- read_feather("data/taxi.feather")

tax %>% 
  arrow_table() %>% 
  mutate(year2 = year(pickup_datetime)) %>% 
  #group_by(vendor_name) %>% 
  summarise(x = arrow_first(passenger_count)) %>% 
  #mutate(f = is.na(rate_code), x = arrow_first(passenger_count)) %>% 
  ungroup() %>% 
  collect()


head(tax)

system.time(ds <- open_dataset("yellow-2023"))
system.time(ds <- open_dataset("yellow-2023") %>% collect())

system.time(xx <- read_csv_arrow("data/taxi.csv"))
system.time(xx <- read_csv_arrow("data/taxi.csv", as_data_frame = FALSE))

system.time(fea <- read_feather("data/taxi.feather", as_data_frame = FALSE))
system.time(feb <- read_feather("data/taxi.feather", as_data_frame = TRUE))


dim(feb)
write_json(
  feb, 
  path = "taxi.json"
)

xx <- read_json_arrow("taxi.json")

obj_size(fea)

obj_size(collect(fea))

tax <- collect(fea)
obj_size(tax)

taxi <- arrow_table(tax)
obj_size(taxi)


ds <- open_dataset("yellow-2023")

ds

system.time({
  a <- ds %>% as_tibble() %>% filter(day > 25, fare_amount > 0, !is.na(passenger_count))
})

system.time({
  b <- ds %>% filter(day > 25, fare_amount > 0, !is.na(passenger_count)) %>% as_tibble()
})

a <- arrange(a, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance)
b <- arrange(b, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance)


identical(a, b)

summary(a$fare_amount - b$fare_amount)

filter(a, fare_amount != b$fare_amount)

system.time({
  ds %>% filter(trip_distance > 20) %>% as_tibble()
})


