# Build project
You should run `mvn install` to build the project (will create `target/` directory).

If you don't have `mvn` installed you can use docker image to build it:
```
docker run -it --rm -v `pwd`:/app maven:3-jdk-14 bash -c 'cd /app; mvn install'
```
