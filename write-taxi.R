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
  "utils::write.csv()" = write.csv(taxi, "taxi.csv"), 
  "readr::write_csv()" = write_csv(taxi, "taxi.csv", progress = FALSE), 
  "vroom::vroom_write()" = vroom_write(taxi, "taxi.csv", progress = FALSE), 
  "data.table::fwrite()" = fwrite(taxi, "taxi.csv"), 
  "arrow::write_csv_arrow()" = write_csv_arrow(taxi, "taxi.csv"), 
  "base::saveRDS()" = saveRDS(taxi, "taxi.rds"), 
  "qs::qsave()" = qsave(taxi, "taxi.qs", nthreads = nt), 
  "fst::write_fst()" = write_fst(taxi, "taxi.fst"),
  "arrow::write_parquet()" = write_parquet(taxi, "taxi.parquet", compression = "lz4"),
  "arrow::write_feather()" = write_feather(taxi, "taxi.feather", compression = "lz4"),
  times = 5
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
