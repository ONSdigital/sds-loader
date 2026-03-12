# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.13-alpine

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Copy the pyproject and lockfile first (for better caching)
ADD pyproject.toml uv.lock ./

# Ensure Lockfile is up to date
RUN uv lock --check

# Install the project's dependencies using the lockfile and settings
# We use docker mounts to cache the UV cache acrross builds (This speeds up multiple Docker builds)
# We also use bind mounts for uv.lock and pyproject.toml to ensure that the cache is invalidated when they change
RUN uv sync --no-install-project --no-dev

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
ADD . /app
RUN uv sync --no-dev


# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Expose the port the app runs on
EXPOSE 5000

# Reset the entrypoint to avoid potentially prefixing the command from other based images.
# i.e ENTRYPOINT ["python"] + CMD ["python", "run.py"] will result in ENTRYPOINT ["python", "python", "run.py"]
ENTRYPOINT []

CMD ["uv", "run", "--no-dev", "python", "run.py"]
