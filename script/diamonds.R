library(data.table)
library(dplyr)
library(here)

data(diamonds, package = "ggplot2")

set.seed(11020)

diamonds20 <- lapply(1:20, function(i) {
  r <- signif(rnorm(nrow(diamonds)), 3)
  mutate(
    diamonds, 
    across(depth:z, ~ .x + r)
  )
}) %>% rbindlist()

fwrite(diamonds20, here("data/diamonds.csv"))

