FROM quay.io/astronomer/astro-runtime:12.7.1

USER root
RUN pip install kaggle apache-airflow-providers-google

COPY include/kaggle.json /root/.kaggle/kaggle.json
RUN chmod 600 /root/.kaggle/kaggle.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/creds.json

USER astro
