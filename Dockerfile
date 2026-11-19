FROM apache/spark:3.5.7-python3

USER root
ADD --checksum=sha256:70f9471870b2d456167d703c829f098a446b86bb1379eee338c0efa03a341026 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
RUN chmod 0644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar && mkdir -p /app
ENV PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
WORKDIR /app
USER 185
# The project is bind-mounted by Compose; no host source/data is baked in.
ENTRYPOINT []
CMD ["/opt/spark/bin/spark-submit", "--help"]
