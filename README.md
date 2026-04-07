# sds-loader

A microservice for loading and modifying data into SDS


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
docker build -t sds-loader .
```


