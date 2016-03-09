# S32RS

Ship S3 to Redshift using COPY command


# Configure

1. Ensure that Redshift can SSH into the host to copy file. Add the SSH key
of Redshift to ~/.ssh/authorized_keys file
2. Install AWS CLI. For simplicity, we use AWS CLI to download/upload s3
   file
3. Create a directory /s32rs, make it writable to user who runs this app

## Enviroment vars

- `PG_USER`          username of redshift
- `PG_PWD`           password
- `PG_DB`            db name
- `PG_HOST`          redshift host
- `PG_PORT`          redshift port
- `AWS_KEY`          an aws key to store manifest
- `AWS_SECRET`       secret key
- `AWS_BUCKET_S32RS` bucket name(above key/secret should have permission
  to write to this)
- `SSH_USER`         username
- `SSH_IP`           public ip

The S3 bucket is a place to store manifest file. Manifest is very small,
just a couple of JSON.
http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html

# Run

Standalone mode to try

```
source run
make build
```

# How to

## Post to server:

```shell
curl --data "project_id=...&aws_key=..&aws_secret=...&s3_bucket=..." https://127.0.0.1:3001/work
```

