FROM python:3.6

#RUN apt-get update -y

#RUN apt-get install libsasl2-dev python-dev libldap2-dev libssl-dev libsnmp-dev -y

#copy all files into docker container

#COPY requirements.txt ${AIRFLOW_USER_HOME}/airflow_docker/dags/requirements.txt

#install requirements
RUN python -m pip install python-dotenv

