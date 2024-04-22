
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
	Rscript script/diamonds.R

taxi: 
	Rscript script/taxi.R
  
write: 
	Rscript script/write.R

write-taxi: 
	Rscript script/write-taxi.R

read:
	Rscript read.R

read-taxi:
	Rscript script/read-taxi.R

size: 
	Rscript script/size.R

.PHONY: results
results: 
	Rscript script/results.R
	

clean: 
	rm *.csv
	rm *.feather
	rm *.parquet
	rm *.rds
	rm *.qs
	rm *.fst
