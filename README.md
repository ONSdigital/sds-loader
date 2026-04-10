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

## OpenApi spec

[http://0.0.0.0:5000/docs](http://0.0.0.0:5000/docs)

## Dockerize

```
docker build -t sds-loader .
```

## Firestore simulator

```

export FIRESTORE_EMULATOR_HOST=localhost:8080
export GCLOUD_PROJECT=ons-sds-sandbox

```
```
docker run \
  --rm \
  -p=9000:9000 \
  -p=8080:8080 \
  -p=4000:4000 \
  -p=9099:9099 \
  -p=8085:8085 \
  -p=5001:5001 \
  -p=9199:9199 \
  --env "GCP_PROJECT=ons-sds-sandbox" \
  --env "ENABLE_UI=true" \
  spine3/firebase-emulator
```



