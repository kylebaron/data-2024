
diamonds: 
	Rscript diamonds.R

taxi: 
	Rscript taxi.R
  
write: 
	Rscript write.R
	
read:
	Rscript read.R
  
clean: 
	rm *.csv
	rm *.feather
	rm *.parquet
	rm *.rds
	rm *.qs
	rm *.fst
