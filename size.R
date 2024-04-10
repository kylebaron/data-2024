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
  Data = "diamonds", 
  Format = c("csv", "rds", "qs", "fst", "parquet", "feather"),
  Size = round(sizes/1000/1000, 1),
  Unit = "MB", 
  Relative = Size / first(Size)
)

dd

saveRDS(dd, file="results/size-results.rds")
