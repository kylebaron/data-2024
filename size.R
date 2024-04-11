library(dplyr)

sizes <- c(
  file.size("diamonds20.csv"), 
  file.size("diamonds20.rds"), 
  file.size("diamonds20.qs"), 
  file.size("diamonds20.fst"), 
  file.size("diamonds20.parquet"),
  file.size("diamonds20.feather")
)

dd <- tibble(
  Format = c("csv", "rds", "qs", "fst", "parquet", "feather"),
  Size = round(sizes/1000/1000, 1),
  Unit = "MB", 
  Relative = Size / first(Size), 
  Data = "diamonds"
)

dd

saveRDS(dd, file="results/size-results.rds")

sizes <- c(
  file.size("taxi.csv"), 
  file.size("taxi.rds"), 
  file.size("taxi.qs"), 
  file.size("taxi.fst"), 
  file.size("taxi.parquet"),
  file.size("taxi.feather")
)

dd <- tibble(
  Format = c("csv", "rds", "qs", "fst", "parquet", "feather"),
  Size = round(sizes/1000/1000, 1),
  Unit = "MB", 
  Relative = Size / first(Size), 
  Data = "taxi"
)

dd

saveRDS(dd, file="results/size-results-taxi.rds")
