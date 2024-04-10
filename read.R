library(vroom)
library(data.table)
library(readr)
library(microbenchmark)
library(qs)
library(arrow)
library(fstcore)
library(fst)
library(dplyr)

diamonds <- fread("data/diamonds.csv")

nt <- 4
arrow::set_cpu_count(nt)
fst::threads_fst(nt)
data.table::setDTthreads(nt)

fwrite(x = diamonds, file = "diamonds20.csv")
qsave(x = diamonds, file = "diamonds20.qs")
saveRDS(diamonds, file = "diamonds20.rds")
write_parquet(x = diamonds, sink = "diamonds20.parquet")
write_feather(x = diamonds, sink = "diamonds20.feather")
write_fst(x = diamonds, path = "diamonds20.fst")


x <- microbenchmark(
  "utils::read.csv()" = read.csv("diamonds20.csv", header = TRUE), 
  "readr::read_csv()" = read_csv("diamonds20.csv", show_col_types = FALSE, progress = FALSE), 
  "vroom::vroom()" = vroom("diamonds20.csv", show_col_types = FALSE, progress = FALSE, num_threads = nt), 
  "data.table::fread()" = fread("diamonds20.csv", nThread=nt), 
  "arrow::read_csv_arrow()" = read_csv_arrow("diamonds20.csv"), 
  "base::readRDS()" = readRDS("diamonds20.rds"), 
  "qs::qread()" = qread("diamonds20.qs", nthreads = nt), 
  "fst::read_fst()" = read_fst("diamonds20.fst"),
  "arrow::read_parquet()" = read_parquet("diamonds20.parquet"), 
  "arrow::read_feather()" = read_feather("diamonds20.feather"),
  times = 10
) 


dd <- 
  data.frame(x) %>% group_by(expr) %>% 
  summarise(Time = mean(time)/1e6, 
            Min = min(time)/1e6, Max = max(time)/1e6, n = n()) %>% 
  mutate(Relative = Time/nth(Time,2)) %>% 
  mutate(across(Time:Max, round)) %>% 
  select(Function = expr, Time, Relative, Min, Max, n)

dd


saveRDS(x, "results/read-bench.rds")
saveRDS(dd, "results/read-results.rds")
