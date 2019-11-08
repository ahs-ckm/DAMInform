go build -ldflags "-X main.gBuild=`date -u +.%Y%m%d.%H%M%S`" ./DAMInform.go 
scp ./DAMInform coni@beeby.ca:~/
