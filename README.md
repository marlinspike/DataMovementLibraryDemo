## Data Movement Library Demo
This demo of the .NET Data Movement Library demonstrates uploading a single file and a directory recursively. While the Azure Storage API (v12) provides all the great 
retry/backoff features you'd imagine, DML adds some advanced features, like setting the number of threads as well as the chunking size of the data.

## Pre-requisites
- An Azure Storage Account (v1, v2 or BlobStorage). You can even use an Azure Storage DataLake Gen2 storage account.
- Follow the instructions below to add a file called *appsettings.json* to the root directory

## Appsettings.json File
```json
{
  "StorageAccount": "<STORAGE_ACCOUNT_NAME_HERE>",
  "StorageKey": "<YOUR_KEY_HERE>",
  "ContainerName": "share",
  "SourceFilePath": "<DEMO_SOURCE_FILE_NAME_HERE>",
  "SourceDirPath": "<DEMO_DIRECTORY_NAME_HERE>",
  "TargetContainer": "upload",
  "ParallelOperations": 8,
  "BlockSize": "20971520"
}```