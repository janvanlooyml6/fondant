FROM --platform=linux/amd64 python:3.8-slim
ARG DEBIAN_FRONTEND=noninteractive

# System dependencies
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install git -y

# Install cv2 specific dependencies
RUN apt-get install ffmpeg libsm6 libxext6  -y

# Install requirements
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Install Fondant
# This is split from other requirements to leverage caching
ARG FONDANT_VERSION=147ce7c9f509c230124142c1c20f5dc32e7e8459
RUN pip3 install git+https://github.com/ml6team/fondant@${FONDANT_VERSION}

# Set the working directory to the component folder
WORKDIR /component/src

# Copy over src-files
COPY src/ .

ENTRYPOINT ["python", "main.py"]