library(vroom)
library(data.table)
library(readr)
library(microbenchmark)
library(qs)
library(arrow)
library(fst)
library(dplyr)

diamonds <- fread("data/diamonds.csv")

nt <- 4
arrow::set_cpu_count(nt)
fst::threads_fst(nt)
data.table::setDTthreads(nt)

x <- microbenchmark(
  "utils::write.csv()" = write.csv(diamonds, "diamonds20.csv"), 
  "readr::write_csv()" = write_csv(diamonds, "diamonds20.csv", progress = FALSE), 
  "vroom::vroom_write()" = vroom_write(diamonds, "diamonds20.csv", progress = FALSE), 
  "data.table::fwrite()" = fwrite(diamonds, "diamonds20.csv"), 
  "arrow::write_csv_arrow()" = write_csv_arrow(diamonds, "diamonds20.csv"), 
  "base::saveRDS()" = saveRDS(diamonds, "diamonds20.rds"), 
  "qs::qsave()" = qsave(diamonds, "diamonds20.qs", nthreads = nt), 
  "fst::write_fst()" = write_fst(diamonds, "diamonds20.fst"),
  "arrow::write_parquet()" = write_parquet(diamonds, "diamonds20.parquet", compression = "lz4"),
  "arrow::write_feather()" = write_feather(diamonds, "diamonds20.feather", compression = "lz4"),
  times = 5
) 

dd <- 
  data.frame(x) %>% group_by(expr) %>% 
  summarise(Time = mean(time)/1e6, 
            Min = min(time)/1e6, Max = max(time)/1e6, n = n()) %>% 
  mutate(Relative = Time/nth(Time,2)) %>% 
  mutate(across(Time:Max, round), Data = "diamonds") %>% 
  select(Function = expr, Time, Relative, Min, Max, Data, n)

dd

saveRDS(x, "results/write-bench.rds")
saveRDS(dd, "results/write-results.rds")
