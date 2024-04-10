
diamonds: 
	Rscript diamonds.R

taxi: 
	Rscript taxi.R
  
write: 
	Rscript write.R
	
read:
	Rscript read.R

size: 
	Rscript size.R

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
