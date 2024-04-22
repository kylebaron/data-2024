library(vroom)
library(data.table)
library(readr)
library(microbenchmark)
library(qs)
library(arrow)
library(fst)
library(dplyr)

taxi <- readRDS("data/taxi.rds")

nt <- 4
arrow::set_cpu_count(nt)
fst::threads_fst(nt)
data.table::setDTthreads(nt)

x <- microbenchmark(
  "utils::write.csv()"       = write.csv(taxi, "tmp/taxi-write.csv"), 
  "readr::write_csv()"       = write_csv(taxi, "tmp/taxi-write.csv", progress = FALSE), 
  "data.table::fwrite()"     = fwrite(taxi, "tmp/taxi-write.csv"), 
  "vroom::vroom_write()"     = vroom_write(taxi, "tmp/taxi-write.csv", progress = FALSE), 
  "arrow::write_csv_arrow()" = write_csv_arrow(taxi, "tmp/taxi-write.csv"), 
  "base::saveRDS()"          = saveRDS(taxi, "tmp/taxi-write.rds"), 
  "qs::qsave()"              = qsave(taxi, "tmp/taxi-write.qs", nthreads = nt), 
  "fst::write_fst()"         = write_fst(taxi, "tmp/taxi-write.fst"),
  "arrow::write_feather()"   = write_feather(taxi, "tmp/taxi-write.feather", compression = "lz4"),
  "arrow::write_parquet()"   = write_parquet(taxi, "tmp/taxi-write.parquet", compression = "lz4"),
  times = 10
) 

dd <- 
  data.frame(x) %>% group_by(expr) %>% 
  summarise(Time = mean(time)/1e6, 
            Min = min(time)/1e6, Max = max(time)/1e6, n = n()) %>% 
  mutate(Relative = Time/nth(Time,2), Data = "taxi") %>% 
  mutate(across(Time:Max, round)) %>% 
  select(Function = expr, Time, Relative, Min, Max, Data, n)

dd

saveRDS(x, "results/write-taxi-bench.rds")
saveRDS(dd, "results/write-taxi-results.rds")
