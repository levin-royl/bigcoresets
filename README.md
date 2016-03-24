# bigcoresets

example of input params
-----------------------
--sparkParams spark.master=local[*] -v -i /Users/royl/data/coresets/streamin -o /Users/royl/data/coresets/out/artho.vec -a coreset-kmeans --algorithmParams 100 --sampleSize 1024 --batchSecs 10 --parallelism 16 --denseData --checkpointDir /Users/royl/temp -m streaming