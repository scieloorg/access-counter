FROM python:3.9.1-alpine3.12 AS build
COPY . /src
RUN pip install --upgrade pip \
    && pip install wheel
RUN cd /src \
    && python setup.py bdist_wheel -d /deps

FROM python:3.9.1-alpine3.12
LABEL MAINTAINER scielo-dev@googlegroups.com

COPY --from=build /deps/* /deps/
COPY requirements.txt .

RUN apk add --no-cache --virtual .build-deps gcc g++ \
    && apk add --no-cache lapack lapack-dev blas blas-dev libstdc++ gfortran \
    && apk add --no-cache mariadb-dev libxslt-dev \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-index --find-links=file:///deps -U scielo-usage-counter \
    && apk --purge del .build-deps \
    && rm -rf /deps

WORKDIR /app

CMD ["calculate_metrics", "--help"]
