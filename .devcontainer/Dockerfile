FROM mcr.microsoft.com/vscode/devcontainers/python:3.10

RUN pip install \
    plotly \
    pandas \
    requests \
    jupyter \
    websocket_client \
    aiocometd \
    thetadata

RUN apt-get update && apt-get install -y openjdk-17-jdk

EXPOSE 8888
