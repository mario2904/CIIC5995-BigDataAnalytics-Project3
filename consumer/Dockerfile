FROM mario2904/hadoop

# Install Hive
ENV HIVE_VERSION=2.1.1
ENV HIVE_HOME=/usr/local/hive
ENV PATH=$PATH:$HIVE_HOME/bin
RUN wget http://www-us.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz && \
    tar xvf apache-hive-$HIVE_VERSION-bin.tar.gz && \
    mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME && \
    rm apache-hive-$HIVE_VERSION-bin.tar.gz $HIVE_HOME/lib/log4j-slf4j-impl-2.4.1.jar

# Install Spark
ENV SPARK_VERSION=2.1.0
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
RUN wget http://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VERSION-bin-without-hadoop.tgz && \
    tar xvf spark-$SPARK_VERSION-bin-without-hadoop.tgz && \
    mv spark-$SPARK_VERSION-bin-without-hadoop $SPARK_HOME && \
    rm spark-$SPARK_VERSION-bin-without-hadoop.tgz

# Configure Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONIOENCODING=utf8
RUN cp $SPARK_CONF_DIR/spark-env.sh.template $SPARK_CONF_DIR/spark-env.sh && \
    echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> $SPARK_CONF_DIR/spark-env.sh

RUN apt-get update && apt-get install -y unzip python3-pip
RUN pip3 install numpy
CMD ["tail", "-f", "/dev/null"]
