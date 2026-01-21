FROM public.ecr.aws/lambda/python:3.12

WORKDIR /var/task

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY src/pyrolysis_aws /var/task/pyrolysis_aws

CMD ["pyrolysis_aws.lambdas.tile_worker.handler.handler"]
