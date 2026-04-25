FROM python:3.14

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# Set working directory
WORKDIR /code

# Copy dependency files from root
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies using uv
RUN uv sync

RUN uv add fastapi[standard]

# Copy all Python scripts
COPY src/FastAPI_api.py /code/FastAPI_api.py
COPY src/worker.py /code/worker.py
COPY src/jobs.py /code/jobs.py

ENV PATH="/code:$PATH"

# Default to FastAPI app, but can be overridden with docker run --entrypoint
CMD ["uv", "run", "--", "fastapi", "dev",  "--host", "0.0.0.0", "FastAPI_api.py"]