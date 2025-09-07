FROM jupyter/pyspark-notebook:latest

USER root

RUN pip install --no-cache-dir \
    matplotlib \
    seaborn \
    numpy \
    pandas

COPY customer_analysis.py /home/jovyan/work/
COPY spark_config.py /home/jovyan/work/

WORKDIR /home/jovyan/work

USER $NB_UID

CMD ["spark-submit", "--master", "local[*]", "customer_analysis.py"]
