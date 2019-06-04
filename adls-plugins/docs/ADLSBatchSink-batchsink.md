# Azure Data Lake Store Batch Sink

Description
-----------
Azure Data Lake Store Batch Sink writes data to Azure Data Lake Store directory in avro, orc or text format.

Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path** Path to directory to store output files. The path must start with `adl://`

*NOTE* 
Either of configs {keyVaultUrl,kvKeyNames} OR {clientId,refreshTokenURL,credentials} MUST be specified.
If former information is available, later configs are NOT required. Infact, former one(keyVault) is a more secured 
approach to avoid specifying sensitive Service Principle details in Plugin config.
Please refer to `DevNote` for using `keyVault` approach.


**keyVaultUrl** Provide Mircroft's KeyVault URL address from where client credentials can be fetched

**kvKeyNames** Provide key names for secret key-values to be fetched from KeyVault store. For connecting to ADLS as ClientCredentials role, a service
principle is required which gets specified by 3 configs - clientId, clientCrdential and RefreshTokenUrl. Therefore, each
specified key name MUST be specified the identifier to which it should be mapped.

**clientId** Microsoft Azure client Id which is typically Application Id
 
**refreshTokenURL** Refresh URL to access Microsoft Azure Data Store 

**credentials** Key to access Microsoft Azure Data Store

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system. (Macro-enabled)

**outputFormat:** The format of output files. Must be 'avro', 'text' or 'orc'.

**schema:** Output schema of the JSON document. Required for avro and orc output formats. If left empty for text output format, the schema of input records will be used. This must be a subset of the schema of input records. Fields of type ARRAY, MAP, and RECORD are not supported with the text format. Fields of type UNION are only supported if they represent a nullable type.

**textDelimiter:** Delimiter to place between fields. Only used by the text output format. Defaults to tab.

DevNote
-------
For using keyVault approach, it is required that underlying platform, on which CDAP is running, must contain a `/etc/security/jceks/adls.jceks`
file which is generated using `hadoop-credential` utility. This `jceks` file is expected to contain values for following `keys`
so as to access `KeyVault` itself - `fs.adl.oauth2.client.id` and `fs.adl.oauth2.credential`.


Example
-------
This example connects to Microsoft Azure Data Lake Store and writes files in avro format to specified path
specified directory. 


(1) This example uses Microsoft Azure Data Lake Store 'adls.azuredatalakestore.net', using the
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


(2) This example uses Microsoft Azure Data Lake Store 'xyz.azuredatalakestore.net', using the
 Credentials which are themselves read from an Azure KeyVault :

    {
        "name": "AzureBlobStore",
        "type": "batchsink",
        "properties": {
            "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"ID\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALLING_NUM\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALLED_NUMBER\",\"type\":[\"string\",\"null\"]},{\"name\":\"START_TIME\",\"type\":[\"string\",\"null\"]},{\"name\":\"END_TIME\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALL_TYPE\",\"type\":[\"string\",\"null\"]},{\"name\":\"CHARGE\",\"type\":[\"string\",\"null\"]},{\"name\":\"CALL_RESULT\",\"type\":[\"string\",\"null\"]}]}",
            "path": "adl://adls.azuredatalakestore.net/adls/cdr/",
            "referenceName": "ADLSBatchSink",
            "keyVaultUrl": "clientId:ClientId_KeyName,clientCredential:ClientCredential_KeyName,endpointUrl:RefreshTokenUrl_KeyName",
            "outputFormat": "avro"
        }
    }
    Note: that in this example, `clientId`, `clientCredential` and `endpointUrl` represents `keyNames` whose values are store in `KeyVault`.

