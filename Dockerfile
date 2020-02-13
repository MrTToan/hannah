FROM python:3.6

RUN apt-get update \
    && apt-get install -qq --no-install-recommends -y \
        build-essential libpq-dev libffi-dev libmariadbclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV APP_HOME /tiki/hannah

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR $APP_HOME

ADD . "${APP_HOME}"
ENV PYTHONPATH=$APP_HOME
