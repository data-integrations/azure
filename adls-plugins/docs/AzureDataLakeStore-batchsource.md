# Azure Data Lake Store Batch Source

Description
-----------
Azure Data Lake Store Batch Source reads data from Azure Data Lake Store files and converts it into 
StructuredRecord.

Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**path** Path files under to Azure Data Lake Store directory

**clientId** Microsoft Azure client Id which is typically Application Id
 
**refreshTokenURL** Refresh URL to access Microsoft Azure Data Store 

**credentials** Key to access Microsoft Azure Data Store

**maxSplitSize:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB. (Macro-enabled)

**fileRegex:** Regex to filter out filenames in the path.
To use the *TimeFilter*, input ``timefilter``. The TimeFilter assumes that it is
reading in files with the File log naming convention of *YYYY-MM-DD-HH-mm-SS-Tag*.
The TimeFilter reads in files from the previous hour if the field ``timeTable`` is
left blank. If it's currently *2015-06-16-15* (June 16th 2015, 3pm), it will read
in files that contain *2015-06-16-14* in the filename. If the field ``timeTable`` is
present, then it will read in files that have not yet been read. (Macro-enabled)

**recursive** Boolean value to determine if files are to be read recursively from the path. Default is false.

**inputFormatClass:** Name of the input format class, which must be a
subclass of FileInputFormat. Defaults to CombineTextInputFormat. (Macro-enabled)

**ignoreNonExistingFolders:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**timeTable:** Name of the Table that keeps track of the last time files
were read in. (Macro-enabled)

**pathField:** If specified, each output record will include a field with this name that contains the file URI
that the record was read from. Requires a customized version of CombineFileInputFormat, so it cannot be used
if an inputFormatClass is given.

**filenameOnly** If true and a pathField is specified, only the filename will be used. If false, the full 
URI will be used. Defaults to false.

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system. (Macro-enabled)


Example
-------
This example connects to Microsoft Azure Data Lake Store and reads in files found in the
specified directory. This example uses Microsoft Azure Data Lake Store 'xyz.azuredatalakestore.net', using the
'clientID', oauth2 refreshTokenURL and Keys as Credentials :

    {
        "name": "AzureBlobStore",
        "type": "batchsource",
        "properties": {
            "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"offset\",\"type\":\"long\"},{\"name\":\"body\",\"type\":\"string\"}]}",
            "recursive": "false",
            "referenceName": "store",
            "path": "adl://xyz.azuredatalakestore.net/adls",
            "clientId": "2016c0cb-9b0a-411d-9976-457112a6baca",
            "refreshTokenURL": "https://login.windows.net/6f3d9678-d0b4-4d7e-ac55-128e30605fac/oauth2/token",
            "credentials": "d1cF7CwFJKlMWXPz30OZ0XD8DErPsSWf0zXyH4iDzKA="
        }
    }
