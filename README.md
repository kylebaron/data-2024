
# Apache Arrow Based Workflows for Large Data Analytics

Slides are availble in [pdf](apache-arrow-april-2024.pdf) format

[arrow.qmd](arrow.qmd) runs simulations with mrgsolve and processes outputs 
with Apache Arrow

See the [Makefile](Makefile) for tasks

- `make taxi` calls `script/taxi.R` to download and save tiny NYC taxi data

- `make read-taxi` runs read benchmarks for NYC taxi data via `script/read-taxi.R`

- `make write-taxi` runs write benchmarks for NYC taxi data via `script/write-taxi.R`

- `make size` compares file sizes via `script/size.R`


