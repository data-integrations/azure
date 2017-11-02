# Azure Data Lake Store Batch Sink

Description
-----------
Azure Data Lake Store Batch Sink writes data to Azure Data Lake Store directory in avro format.

Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path** Path to directory to store output files. The path must start with `adl://`

**clientId** Microsoft Azure client Id which is typically Application Id
 
**refreshTokenURL** Refresh URL to access Microsoft Azure Data Store 

**credentials** Key to access Microsoft Azure Data Store

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system. (Macro-enabled)

**schema:** Output schema for the JSON document.


Example
-------
This example connects to Microsoft Azure Data Lake Store and writes files in avro format to specified path
specified directory. This example uses Microsoft Azure Data Lake Store 'adls.azuredatalakestore.net', using the
'clientID', oauth2 refreshTokenURL and Keys as Credentials :

    {
        "name": "AzureBlobStore",
        "type": "batchsource",
        "properties": {
            "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"ID\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALLING_NUM\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALLED_NUMBER\",\"type\":[\"string\",\"null\"]},{\"name\":\"START_TIME\",\"type\":[\"string\",\"null\"]},{\"name\":\"END_TIME\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALL_TYPE\",\"type\":[\"string\",\"null\"]},{\"name\":\"CHARGE\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALL_RESULT\",\"type\":[\"string\",\"null\"]}]}",
            "path": "adl://adls.azuredatalakestore.net/adls/cdr/",
            "referenceName": "ADLSBatchSink",
            "clientId": "1016c0cb-9b0a-411d-9979-457550a7baca",
            "refreshTokenURL": "https://login.windows.net/5f3d9a6a-d054-4d7e-ac55-128e30605f6c/oauth2/token",
            "credentials": "f1cF7CwFJKlMWXPz30OZ1XB7BErQsSWf0zXyH4iDzKA="
        }
    }
