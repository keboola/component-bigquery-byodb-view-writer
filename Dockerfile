FROM python:3.11-slim
ENV PYTHONIOENCODING utf-8

COPY /src /code/src/
COPY /tests /code/tests/
COPY /scripts /code/scripts/
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh
COPY pyproject.toml uv.lock /code/

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
RUN apt-get update && apt-get install -y git

# Install build tools
RUN pip install --no-cache-dir uv flake8

# Install dependencies
WORKDIR /code
RUN uv pip install -r pyproject.toml --system --no-cache

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]
