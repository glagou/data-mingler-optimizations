FROM python:3.9.6 as build
WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}
RUN pip install --upgrade pip
RUN pip install jep
ENV JEP_PATH=/usr/local/lib/python3.9/site-packages/jep/jep.cp39-win_amd64.dll

FROM maven:3.9
WORKDIR /app
COPY --from=build /app /app
# RUN mvn install
