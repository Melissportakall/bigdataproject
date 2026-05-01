# Temel imaj olarak PySpark yüklü Jupyter Notebook kullanıyoruz
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
RUN apt-get update && apt-get install -y gcc

USER $NB_UID
# Proje için gerekli olan Python kütüphanelerini kuruyoruz
RUN pip install kafka-python mlflow delta-spark pandas