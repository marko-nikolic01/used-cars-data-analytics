FROM bitnami/spark:3.2.2

# Install SSH and setup password-based authentication
USER root
RUN apt-get update && apt-get install -y openssh-server \
    && mkdir -p /run/sshd \
    && ssh-keygen -A \
    && echo 'root:rootpassword' | chpasswd \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && service ssh start

# Start Spark Master and SSH
CMD /usr/sbin/sshd -D & /opt/bitnami/spark/sbin/start-master.sh
