# Azure Data Lake Store Batch Sink

Description
-----------
Azure Data Lake Store Batch Sink writes data to Azure Data Lake Store directory in avro, orc or text format.

Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path** Path to directory to store output files. The path must start with `adl://`

**clientId** Microsoft Azure client Id which is typically Application Id
 
**refreshTokenURL** Refresh URL to access Microsoft Azure Data Store 

**credentials** Key to access Microsoft Azure Data Store

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system. (Macro-enabled)

**outputFormat:** The format of output files. Must be 'avro', 'text' or 'orc'.

**schema:** Output schema of the JSON document. Required for avro and orc output formats. If left empty for text output format, the schema of input records will be used. This must be a subset of the schema of input records. Fields of type ARRAY, MAP, and RECORD are not supported with the text format. Fields of type UNION are only supported if they represent a nullable type.

**textDelimiter:** Delimiter to place between fields. Only used by the text output format. Defaults to tab.

Example
-------
This example connects to Microsoft Azure Data Lake Store and writes files in avro format to specified path
specified directory. This example uses Microsoft Azure Data Lake Store 'adls.azuredatalakestore.net', using the
'clientID', oauth2 refreshTokenURL and Keys as Credentials :

    {
        "name": "AzureBlobStore",
        "type": "batchsink",
        "properties": {
            "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"ID\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALLING_NUM\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALLED_NUMBER\",\"type\":[\"string\",\"null\"]},{\"name\":\"START_TIME\",\"type\":[\"string\",\"null\"]},{\"name\":\"END_TIME\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALL_TYPE\",\"type\":[\"string\",\"null\"]},{\"name\":\"CHARGE\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALL_RESULT\",\"type\":[\"string\",\"null\"]}]}",
            "path": "adl://adls.azuredatalakestore.net/adls/cdr/",
            "referenceName": "ADLSBatchSink",
            "clientId": "1016c0cb-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "refreshTokenURL": "https://login.windows.net/5f3d9a6a-aaaa-aaaa-aaaa-aaaaaaaaaaaa/oauth2/token",
            "credentials": "f1cF7CwFJKlMWXPzAAAA1XB7BErAAAAAAAAAAAAAAAA=",
            "outputFormat": "avro"
        }
    }
    
Here is another example with the same configuration as the above example except the **outputFormat** is "text" 
and **textDelimiter** is "," :

    {
        "name": "AzureBlobStore",
        "type": "batchsink",
        "properties": {
            "path": "adl://adls.azuredatalakestore.net/adls/cdr/",
            "referenceName": "ADLSBatchSink",
            "clientId": "1016c0cb-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "refreshTokenURL": "https://login.windows.net/5f3d9a6a-aaaa-aaaa-aaaa-aaaaaaaaaaaa/oauth2/token",
            "credentials": "f1cF7CwFJKlMWXPzAAAA1XB7BErAAAAAAAAAAAAAAAA=",
            "outputFormat": "text",
            "textDelimiter": ","
        }
    }
