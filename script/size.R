library(dplyr)
library(qs)
library(arrow)

sizes <- c(
  file.size("data/taxi.csv"), 
  file.size("data/taxi.rds"), 
  file.size("data/taxi.qs"), 
  file.size("data/taxi.fst"), 
  file.size("data/taxi.parquet"),
  file.size("data/taxi.feather")
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


sizes <- c(
  file.size("data/taxi.csv"), 
  file.size("data/taxi.feather"), 
  file.size("data/taxi.parquet"),
  file.size("data/taxi-uncompressed.feather"), 
  file.size("data/taxi-uncompressed.parquet")
)

dd <- tibble(
  Format = c("csv", "feather", "parquet", "feather", "parquet"),
  Comp = c("no", "yes", "yes", "no", "no"), 
  Size = round(sizes/1000/1000, 1),
  Unit = "MB", 
  Relative = Size / first(Size), 
  Data = "taxi"
)

dd

saveRDS(dd, file="results/size-results-taxi-comp.rds")


x <- read_parquet(
  "data/taxi.parquet", 
  as_data_frame = FALSE
)

temp1 <- tempfile()
temp2 <- tempfile()
temp3 <- tempfile()
temp4 <- tempfile()

write_parquet(x, temp1)
write_parquet(x, temp2, compression = "uncompressed")
write_parquet(x, temp3, use_dictionary = FALSE)
write_parquet(x, temp4, use_dictionary = FALSE, compression = "uncompressed")

sizes <- c(
  file.size(temp1), 
  file.size(temp2), 
  file.size(temp3),
  file.size(temp4)
)

dd <- tibble(
  Format = c("parquet", "parquet", "parquet", "parquet"),
  Comp = c("yes", "no", "yes", "no"), 
  Dict = c("yes", "yes", "no", "no"), 
  Size = round(sizes/1000/1000, 1),
  Unit = "MB", 
  Relative = Size / first(Size), 
  Data = "taxi"
)

dd

saveRDS(dd, file="results/size-results-taxi-comp-dict.rds")
