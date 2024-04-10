library(vroom)
library(data.table)
library(readr)
library(microbenchmark)
library(qs)
library(arrow)
library(fstcore)
library(fst)
library(dplyr)

taxi <- readRDS("data/taxi.rds")

nt <- 4
arrow::set_cpu_count(nt)
fst::threads_fst(nt)
data.table::setDTthreads(nt)

fwrite(x = taxi, file = "taxi-read.csv")
qsave(x = taxi, file = "taxi-read.qs")
saveRDS(taxi, file = "taxi-read.rds")
write_parquet(x = taxi, sink = "taxi-read.parquet")
write_feather(x = taxi, sink = "taxi-read.feather")
write_fst(x = taxi, path = "taxi-read.fst")


x <- microbenchmark(
  "utils::read.csv()" = read.csv("taxi-read.csv", header = TRUE), 
  "readr::read_csv()" = read_csv("taxi-read.csv", show_col_types = FALSE, progress = FALSE), 
  "vroom::vroom()" = vroom("taxi-read.csv", show_col_types = FALSE, progress = FALSE, num_threads = nt), 
  "data.table::fread()" = fread("taxi-read.csv", nThread=nt), 
  "arrow::read_csv_arrow()" = read_csv_arrow("taxi-read.csv"), 
  "base::readRDS()" = readRDS("taxi-read.rds"), 
  "qs::qread()" = qread("taxi-read.qs", nthreads = nt), 
  "fst::read_fst()" = read_fst("taxi-read.fst"),
  "arrow::read_parquet()" = read_parquet("taxi-read.parquet"), 
  "arrow::read_feather()" = read_feather("taxi-read.feather"),
  times = 5
) 


dd <- 
  data.frame(x) %>% group_by(expr) %>% 
  summarise(Time = mean(time)/1e6, 
            Min = min(time)/1e6, Max = max(time)/1e6, n = n()) %>% 
  mutate(Data = "taxi") %>% 
  mutate(Relative = Time/nth(Time,2)) %>% 
  mutate(across(Time:Max, round)) %>% 
  select(Function = expr, Time, Relative, Min, Max, Data, n)

dd


saveRDS(x, "results/read-taxi-bench.rds")
saveRDS(dd, "results/read-taxi-results.rds")
