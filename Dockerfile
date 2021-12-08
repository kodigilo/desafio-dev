FROM maven:3.6.0-jdk-11-slim AS build
COPY ./backend/pom.xml /app/
COPY ./backend/src /app/src 
RUN mvn -f /app/pom.xml clean package -DskipTests

FROM tomcat:9-jre11-slim
RUN rm -rf /usr/local/tomcat/webapps/
COPY --from=build /app/target/*.war /usr/local/tomcat/webapps/ROOT.war
RUN echo "America/Sao_Paulo" > /etc/timezone
ENV TZ America/Sao_Paulo