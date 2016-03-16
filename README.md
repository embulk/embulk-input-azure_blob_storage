# Azure blob Storage file input plugin for Embulk

[Embulk](http://www.embulk.org/) file input plugin read files stored on [Microsoft Azure](https://azure.microsoft.com/) [blob Storage](https://azure.microsoft.com/en-us/documentation/articles/storage-introduction/#blob-storage)

## Overview

* **Plugin type**: file input
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

First, create Azure [Storage Account](https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/).

- **account_name**: storage account name (string, required)
- **account_key**: primary access key (string, required)
- **container**: container name data stored (string, required)
- **path_prefix**: prefix of target keys (string, required) (string, required)

## Example

```yaml
in:
  type: azure_blob_storage
  account_name: myaccount
  account_key: myaccount_key
  container: my-container
  path_prefix: logs/csv-
```

Example for "sample_01.csv.gz" , generated by [embulk example](https://github.com/embulk/embulk#trying-examples)

```yaml
in:
  type: azure_blob_storage
  account_name: myaccount
  account_key: myaccount_key
  container: my-container
  path_prefix: logs/csv-
  decoders:
  - {type: gzip}
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    delimiter: ','
    quote: '"'
    header_line: true
    columns:
    - {name: id, type: long}
    - {name: account, type: long}
    - {name: time, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}
    - {name: purchase, type: timestamp, format: '%Y%m%d'}
    - {name: comment, type: string}
out: {type: stdout}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Test

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```

To run unit tests, we need to configure the following environment variables.

Additionally, following files will be needed to upload to existing GCS bucket.

* [sample_01.csv](src/test/resources/sample_01.csv)
* [sample_02.csv](src/test/resources/sample_02.csv)

When environment variables are not set, skip some test cases.

```
AZURE_ACCOUNT_NAME
AZURE_ACCOUNT_KEY
AZURE_CONTAINER
AZURE_CONTAINER_IMPORT_DIRECTORY (optional, if needed)
```

If you're using Mac OS X El Capitan and GUI Applications(IDE), like as follows.
```xml
$ vi ~/Library/LaunchAgents/environment.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>my.startup</string>
  <key>ProgramArguments</key>
  <array>
    <string>sh</string>
    <string>-c</string>
    <string>
      launchctl setenv AZURE_ACCOUNT_NAME my-account-name
      launchctl setenv AZURE_ACCOUNT_KEY my-account-key
      launchctl setenv AZURE_CONTAINER my-container
      launchctl setenv AZURE_CONTAINER_IMPORT_DIRECTORY unittests
    </string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>

$ launchctl load ~/Library/LaunchAgents/environment.plist
$ launchctl getenv AZURE_ACCOUNT_NAME //try to get value.

Then start your applications.
```