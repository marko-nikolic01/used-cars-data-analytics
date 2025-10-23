FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y openssh-server && \
    mkdir -p /run/sshd && \
    ssh-keygen -A && \
    echo 'root:rootpassword' | chpasswd && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./scripts/extract_batch_used_cars_data.py .
COPY ./scripts/extract_stream_used_cars_data.py .

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]
