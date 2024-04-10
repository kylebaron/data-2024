library(vroom)
library(data.table)

system.time(v <- vroom("data/diamonds.csv", show_col_types = FALSE, progress = FALSE))
system.time(d <- fread("data/diamonds.csv", nThread=8))

system.time(v <- vroom("data/diamonds.csv", show_col_types = FALSE, progress = FALSE, altrep = FALSE))
system.time(v <- vroom("data/diamonds.csv", show_col_types = FALSE, progress = FALSE, num_threads=1))


system.time(d <- fread("data/diamonds.csv", nThread=1))
