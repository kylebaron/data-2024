library(vroom)
library(data.table)
library(arrow)
library(lobstr)
library(readr)

file <- "data/taxi.csv"

df <- read_csv(file)
obj_size(df)

v <- vroom(file)
lobstr::obj_size(v)
head(v)
lobstr::obj_size(v)
tail(v)
lobstr::obj_size(v)

dt <- as.data.table(v)
lobstr::obj_size(dt)
lobstr::obj_size(df)


system.time(q <- qread("taxi.qs", use_alt_rep = TRUE, nthreads = 16))
lobstr::obj_size(q)
obj_size(as.data.table(q))
system.time(qread("taxi.qs", nthreads = 1))
system.time(qread("taxi.qs", nthreads = 16))


a <- system.time(
  v <- vroom(file, show_col_types = FALSE, progress = FALSE)
)
b <- system.time(
  v2 <- vroom(file, show_col_types = FALSE, progress = FALSE, altrep = FALSE)
)
a[3]/b[3]

lobstr::obj_size(v)
lobstr::obj_size(v2)
lobstr::obj_size(as.data.table(v))


system.time(d <- fread(file))
system.time(a <- read_csv_arrow(file))

system.time(v <- vroom("data/diamonds.csv", show_col_types = FALSE, progress = FALSE, altrep = FALSE))
system.time(v <- vroom("data/diamonds.csv", show_col_types = FALSE, progress = FALSE, num_threads=1))


system.time(d <- fread("data/diamonds.csv", nThread=1))
