FROM amazonlinux:latest


RUN ulimit -n 1024 && yum -y update && yum -y install \
    git \
    gcc \
    python37 \
    python3-devel \
    zip \
    && yum -y groupinstall "Development Tools" \
    && yum clean all

RUN python3 -m pip install --upgrade pip \
    # boto3 is available to lambda processes by default,
    # but it's not in the amazonlinux image
    && python3 -m pip install boto3

# Make it possible to build numpy:
# https://github.com/numpy/numpy/issues/14147
ENV CFLAGS=-std=c99