FROM tikivn/python:3.6

RUN apt-get update \
    && apt-get install -qq --no-install-recommends -y \
        build-essential libpq-dev libffi-dev libmariadbclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD . "${APP_HOME}"

RUN pip install --no-cache-dir -r ./requirements.txt

RUN chown -R "${APP_USER}":"${APP_GRP}" "${APP_HOME}"

WORKDIR $APP_HOME
