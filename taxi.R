library(vroom)
library(data.table)
library(readr)
library(microbenchmark)
library(qs)
library(arrow)
library(fst)
library(dplyr)

bucket <- s3_bucket("voltrondata-labs-datasets/nyc-taxi-tiny")
copy_files(from = bucket, to = "nyc-taxi")

ds <- open_dataset("nyc-taxi")
data <- collect(ds)

fwrite(data, "data/taxi.csv")
qsave(data, "data/taxi.qs")
saveRDS(data, "data/taxi.rds")
saveRDS(data, "data/taxi-bzip.rds", compress = "bzip2")
write_feather(data, "data/taxi.feather")
write_parquet(data, "data/taxi.parquet")
write_fst(data, "data/taxi.fst")

sizes <- c(
  file.size("data/taxi.csv"), 
  file.size("data/taxi.rds"), 
  file.size("data/taxi.qs"), 
  file.size("data/taxi.fst"), 
  file.size("data/taxi.parquet"), 
  file.size("data/taxi.feather")
)

dd <- tibble(
  Data = "taxi-tiny",
  Format = c("csv", "rds", "qs", "fst", "parquet", "feather"),
  Size = round(sizes/1000/1000, 1),
  Unit = "MB", 
  Relative = Size / first(Size)
)

dd

saveRDS(dd, file = "results/results-size-taxi.rds")



