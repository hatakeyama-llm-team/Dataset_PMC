


FROM python:3.9.15

WORKDIR /app
COPY requirements.txt /app
COPY src/ /app
RUN pip install -r requirements.txt
RUN pip install  google-cloud-aiplatform
RUN pip install  "apache-beam[gcp]==2.43.0"

RUN wget https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.4/en_core_sci_lg-0.5.4.tar.gz

RUN tar -zxvf /app/en_core_sci_lg-0.5.4.tar.gz

COPY --from=apache/beam_python3.9_sdk:2.43.0 /opt/apache/beam /opt/apache/beam

ENTRYPOINT ["/opt/apache/beam/boot"]
