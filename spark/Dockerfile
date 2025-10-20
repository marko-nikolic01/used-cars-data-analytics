FROM apache/spark:3.5.2

USER root

RUN apt-get update && \
    apt-get install -y openssh-server rsync curl gnupg && \
    mkdir -p /run/sshd && \
    ssh-keygen -A && \
    echo 'root:rootpassword' | chpasswd && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

RUN curl -sL https://repo.scala-sbt.org/scalasbt/debian/sbt-1.9.9.deb -o sbt.deb && \
    apt-get install -y ./sbt.deb && \
    rm sbt.deb

COPY ./scripts /opt/spark/jobs

RUN for job in /opt/spark/jobs/*; do \
      if [ -d "$job/src/main/scala" ]; then \
        echo "Compiling Scala job in $job..."; \
        cd "$job"; \
        sbt package; \
      fi; \
    done

COPY ./init/spark-entrypoint.sh /opt/spark/spark-entrypoint.sh
RUN chmod +x /opt/spark/spark-entrypoint.sh

ENTRYPOINT ["/opt/spark/spark-entrypoint.sh"]
