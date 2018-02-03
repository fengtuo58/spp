# to build your image
docker build tag hive-spark:1 . 

# to run a container, if using public cloud, make sure the required ports are opened
docker run --rm -e USER=`id -u -n` -e USER_ID=`id -u` -it -v `pwd`/data:/data/hive -p 9083:9083 -p 4040:4040 -p 10000:10000 -p 8888:8888 hive-spark:1

# to access sparck GUI 
http://ip:4040
