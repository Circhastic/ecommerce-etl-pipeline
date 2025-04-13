FROM quay.io/astronomer/astro-runtime:12.7.1

USER root

# Install required packages
RUN pip install kaggle apache-airflow-providers-google

# Copy Kaggle credentials
RUN mkdir -p /home/astro/.kaggle
COPY include/kaggle.json /home/astro/.kaggle/kaggle.json
RUN chown -R astro:astro /home/astro/.kaggle && chmod 600 /home/astro/.kaggle/kaggle.json
ENV KAGGLE_CONFIG_DIR=/home/astro/.kaggle

USER astro