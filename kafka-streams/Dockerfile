FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /build

COPY apps ./apps

RUN cd apps/clean-used-cars-data && mvn clean package -DskipTests
RUN cd apps/calculate-average-price-trends-per-state && mvn clean package -DskipTests
RUN cd apps/calculate-average-price-trends-per-model && mvn clean package -DskipTests

FROM openjdk:17-jdk-slim
USER root
WORKDIR /app

RUN apt-get update && \
    apt-get install -y openssh-server && \
    mkdir -p /run/sshd && \
    ssh-keygen -A && \
    echo 'root:rootpassword' | chpasswd && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

COPY --from=builder /build/apps/*/target/*.jar ./ 

CMD ["/usr/sbin/sshd", "-D"]