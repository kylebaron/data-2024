library(vroom)
library(data.table)
library(readr)
library(microbenchmark)
library(qs)
library(arrow)
library(fstcore)
library(fst)
library(dplyr)

taxi <- read_feather("data/taxi.feather")

nt <- 4
arrow::set_cpu_count(nt)
fst::threads_fst(nt)
data.table::setDTthreads(nt)

fwrite(x = taxi, file = "tmp/taxi-read.csv")
qsave(x = taxi, file = "tmp/taxi-read.qs")
saveRDS(taxi, file = "tmp/taxi-read.rds")
write_parquet(x = taxi, sink = "tmp/taxi-read.parquet")
write_feather(x = taxi, sink = "tmp/taxi-read.feather")
write_fst(x = taxi, path = "tmp/taxi-read.fst")
write_feather(taxi, sink = "tmp/taxi-read-uncompressed.feather", compression = "uncompressed")
write_parquet(taxi, sink = "tmp/taxi-read-uncompressed.parquet", compression = "uncompressed")


x <- microbenchmark(
  "utils::read.csv()" = read.csv("tmp/taxi-read.csv", header = TRUE), 
  "readr::read_csv()" = read_csv("tmp/taxi-read.csv", show_col_types = FALSE, progress = FALSE), 
  "data.table::fread()" = fread("tmp/taxi-read.csv", nThread=nt), 
  "vroom::vroom()" = vroom("tmp/taxi-read.csv", show_col_types = FALSE, progress = FALSE, num_threads = nt), 
  "arrow::read_csv_arrow()" = read_csv_arrow("tmp/taxi-read.csv"), 
  "base::readRDS()" = readRDS("tmp/taxi-read.rds"), 
  "qs::qread()" = qread("tmp/taxi-read.qs", nthreads = nt), 
  "fst::read_fst()" = read_fst("tmp/taxi-read.fst"),
  "arrow::read_feather()" = read_feather("tmp/taxi-read.feather"),
  "arrow::read_parquet()" = read_parquet("tmp/taxi-read.parquet"), 
  times = 10
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
