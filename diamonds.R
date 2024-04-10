library(data.table)
library(dplyr)

data(diamonds, package = "ggplot2")

diamonds20 <- lapply(1:20, function(i) diamonds) %>% rbindlist()

fwrite(diamonds20, "data/diamonds.csv")

