FROM python:3.12.5

# set up location of code
WORKDIR /opt/sahi/src/loader

# lets us import from folders inside /src/loader directly
ENV PYTHONPATH=/opt/sahi

# install python requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# COPY our code folder to docker
COPY ../.. /opt/sahi

# keep our docker container running
CMD ["tail", "-f", "/dev/null"]
