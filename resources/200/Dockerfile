FROM python:3.9.5-buster

COPY . .

RUN apt-get install apt-transport-https ca-certificates gnupg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt-get update && apt-get install google-cloud-cli
RUN apt-get install google-cloud-cli-app-engine-python

ENV GOOGLE_APPLICATION_CREDENTIALS "/opt/code/google-credentials.json"
COPY tenpo-datalake-sandbox-023b3d1dabac.json ${GOOGLE_APPLICATION_CREDENTIALS}
RUN gcloud config set account dbt-sandbox@tenpo-datalake-sandbox.iam.gserviceaccount.com

RUN apt-get update && apt-get install google-cloud-cli
RUN apt-get install google-cloud-cli-app-engine-python

RUN pip install dbt==0.20.0
RUN pip install markupsafe==1.1.1

ENV SENSIBLE_SHA256_PEPPER_SECRET "TEST"
RUN dbt deps --project-dir ./dbt_tenpo_bi/ --profiles-dir ./dbt_tenpo_bi/profiles
RUN dbt compile --project-dir ./dbt_tenpo_bi/ --profiles-dir ./dbt_tenpo_bi/profiles