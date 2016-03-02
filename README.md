# S32RS

Ship S3 to Redshift using COPY command


# Configure

1. Ensure that Redshift can SSH into the host to copy file. Add the SSH key
of Redshift to ~/.ssh/authorized_keys file
2. Install AWS CLI. For simplicity, we use AWS CLI to download/upload s3
   file

# Run

Standalone mode to try

```
go run s.go
```

