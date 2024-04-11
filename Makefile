
all: 
	make diamonds
	make taxi
	make write
	make write-taxi
	make read
	make read-taxi
	make size
	make results

diamonds: 
	Rscript diamonds.R

taxi: 
	Rscript taxi.R
  
write: 
	Rscript write.R

write-taxi: 
	Rscript write-taxi.R

read:
	Rscript read.R

read-taxi:
	Rscript read-taxi.R

size: 
	Rscript size.R
	Rscript size-taxi.R

.PHONY: results
results: 
	Rscript results.R
	

clean: 
	rm *.csv
	rm *.feather
	rm *.parquet
	rm *.rds
	rm *.qs
	rm *.fst
