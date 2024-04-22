library(mrgsolve)
library(arrow)
library(dplyr)
library(glue)

mod <- modlib("popex", end = 24, delta = 1)

dose <-
  expand.ev(amt = c(100, 200, 300, 400), ID = 1:300, rep = 1:3000) %>%
  mutate(dose = amt) %>% 
  as_tibble() %>% 
  mutate(chunk = rep(1:24, length.out = n()))

nrow(dose) * 26

mrgsim(mod, slice(dose, 1)) %>% plot("IPRED")

sp <- split(dose, dose$chunk)

sp[[1]]

unlink("output", recursive = TRUE)

out <- parallel::mclapply(sp, function(chunk) {

  out <- mrgsim_df(mod, chunk, carry_out = "chunk,dose,rep")

  write_dataset(out, "output", partitioning = c("chunk", "dose"))

}, mc.cores = 8)


ds <- open_dataset("output") # instant

ds # load when needed

nrow(ds)

filter(ds, rep==1521)

filter(ds, rep==1521) %>% class()

system.time(filter(ds, rep==1521) %>% collect())

filter(ds, rep==1521) %>% as_tibble()

filter(ds, rep==1521) %>% select(ID, time, IPRED)

filter(ds, rep==1521) %>% select(ID, time, IPRED) %>% collect()

system.time({
  open_dataset("output") %>% 
    filter(rep==1521) %>% 
    collect()
})

system.time({
  open_dataset("output") %>% 
    filter(rep <= 100) %>% 
    collect()
})

# collect(ds) # all the data, as a data frame in memory

#' Get mean trough concentration for 200 mg dose
system.time({
  sims1 <- 
    ds %>% 
    filter(dose==200, time==24)  %>%
    group_by(rep) %>% 
    summarise(Mean = mean(IPRED), .groups = "drop") %>%
    arrange(rep) %>% 
    collect()
}); head(sims1)

#' Get mean trough concentration for all doses
system.time({
  sims1b <- 
    ds %>% 
    filter(time==24)  %>%
    group_by(rep, dose) %>% 
    summarise(Mean = mean(IPRED), n = n(), .groups = "drop") %>%
    arrange(rep, dose) %>% 
    collect()
}); head(sims1b)

#' First collect, then summarise
system.time({
  sims2 <- 
    collect(ds) %>% 
    filter(dose ==200, time==24)  %>%
    group_by(rep) %>% 
    summarise(Mean = mean(IPRED), n = n(), .groups = "drop")
})

#' filter, collect, summarize
system.time({
  sims3 <- 
    ds %>%
    filter(dose==200, time==24) %>% 
    collect() %>% 
    group_by(rep) %>% 
    summarise(Mean = mean(IPRED), n = n(), .groups = "drop")
})

system.time({
  sims5 <- 
    ds %>% 
    group_by(rep, dose, ID) %>% 
    summarise(Cmax = max(IPRED), .groups = "drop") %>% 
    group_by(rep, dose) %>%
    summarise(med = mean(Cmax), .groups = "drop") %>%
    group_by(dose) %>% 
    summarise(Med = median(med), Min = min(med), Max = max(med), n = n()) %>% 
    arrange(dose) %>% 
    collect()
})
dim(ds)
sims5  


system.time({
  sims6 <- 
    ds %>% 
    collect() %>% 
    group_by(rep, dose, ID) %>% 
    summarise(Cmax = max(IPRED), .groups = "drop") %>% 
    group_by(rep, dose) %>%
    summarise(med = mean(Cmax), .groups = "drop") %>%
    group_by(dose) %>% 
    summarise(Med = median(med), Min = min(med), Max = max(med), n = n()) %>% 
    arrange(dose) 
})
sims6 

system.time(x <- collect(ds))

system.time({
  sims7 <- 
    x %>% 
    group_by(rep, dose, ID) %>% 
    summarise(Cmax = max(IPRED), .groups = "drop") %>% 
    group_by(rep, dose) %>%
    summarise(med = mean(Cmax), .groups = "drop") %>%
    group_by(dose) %>% 
    summarise(Med = median(med), Min = min(med), Max = max(med), n = n()) %>%
    arrange(dose)
})
sims7 

system.time(at <- as_arrow_table(x))

system.time({
  sims8 <- 
    at %>%
    group_by(rep, dose, ID) %>% 
    summarise(Cmax = max(IPRED), .groups = "drop") %>% 
    group_by(rep, dose) %>%
    summarise(med = mean(Cmax), .groups = "drop") %>%
    group_by(dose) %>% 
    summarise(Med = median(med), Min = min(med), Max = max(med), n = n()) %>% 
    arrange(dose) %>% 
    collect()
})
sims8 

write_parquet(ds, sink = "ds.parquet")

file.size("ds.parquet") / 1e6

system.time(df <- read_parquet("ds.parquet"))

system.time({
  sims9 <-
    df %>%
    group_by(rep, dose, ID) %>%
    summarise(Cmax = max(IPRED), .groups = "drop") %>%
    group_by(rep, dose) %>%
    summarise(med = mean(Cmax), .groups = "drop") %>%
    group_by(dose) %>%
    summarise(Med = median(med), Min = min(med), Max = max(med), n = n()) %>%
    arrange(dose)
})
sims9
# 
# 
system.time(at <- read_parquet("ds.parquet", as_data_frame=FALSE))

system.time({
  sims10 <-
    at %>%
    group_by(rep, dose, ID) %>%
    summarise(Cmax = max(IPRED), .groups = "drop") %>%
    group_by(rep, dose) %>%
    summarise(med = mean(Cmax), .groups = "drop") %>%
    group_by(dose) %>%
    summarise(Med = median(med), Min = min(med), Max = max(med), n = n()) %>%
    arrange(dose) %>%
    collect()
})
sims10
