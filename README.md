# sdx-fastapi-boilerplate
A boilerplate for creating fastapi services

## Prerequisites

- Python 3.13
- UV (a command line tool for managing Python environments)

### Installing Python 3.13

If you don't have Python 3.13 installed, you can install it via brew:

```bash
brew install python@3.13
```

### Install UV:
   - This project uses UV for dependency management. Ensure it is installed on your system.
   - If UV is not installed, you can install it using:
```bash

curl -LsSf https://astral.sh/uv/install.sh | sh

OR 

brew install uv
```
- Use the official UV installation guide for other installation methods: https://docs.astral.sh/uv/getting-started/installation/
- Verify the installation by using the following command:
```bash
uv --version
```

### Install dependencies

This command will install all the dependencies required for the project, including development dependencies:

```
uv sync
```

If you ever need to update the dependencies, you can run:

```bash
uv sync --upgrade
```

## Generate `.env`

To use environment variables, you need to create a `.env` file in the root directory of the project. You can copy the `.env.example` file and rename it to `.env`:

```bash
cp .env.example .env
```

## Running the service

```bash
make dev
```

## Linting

```bash
make lint
```

## Formatting

```bash
make format
```

## Tests

```bash
make test
```

## Dockerize

```
docker build -t myapp .
```


