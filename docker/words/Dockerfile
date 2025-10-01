FROM ubuntu
# RUN apt-get update -y && apt-get upgrade -y
# RUN apt-get install -y golang-go
RUN apt-get update && apt-get install -y maven openjdk-8-jdk
WORKDIR /app
COPY . .
RUN mvn verify

CMD ["java", "-Xmx8m", "-Xms8m", "-jar","./target/words.jar"]
EXPOSE 8080