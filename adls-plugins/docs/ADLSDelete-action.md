# ADLS Delete Action


Description
-----------
Deletes a file or files within ADLS file system.


Use Case
--------
This action can be used to remove a file or files in an ADLS cluster.


Properties
----------

**fileSystemUri** ADLS file system uri

**refreshTokenURL** Refresh URL to access Microsoft Azure Data Store

**clientId** Microsoft Azure client Id which is typically Application Id
 
**credentials** Key to access Microsoft Azure Data Store

**path:** The full ADLS path of the file or files that need to be deleted. If the path points to a file, 
the file will be removed. If the path points to a directory with no regex specified, the directory and all of 
its contents will be removed. If a regex is specified, only the files and directories matching that regex
will be removed.

**fileRegex:** Wildcard regular expression to filter the files in the source directory that will be removed.

**continueOnError:** Indicates if the pipeline should continue if the delete process fails. If all files are not 
successfully deleted, the action will not re-create the files already deleted.


Example
-------
This example deletes all files ending in `.csv` from `/dirToDelete`:

    {
        "name": "ADLSDelete",
        "type": "action",
        "properties": {
        "fileSystemUri": "adl://adks.azuredatalakestore.net/test",
        "refreshTokenURL": "https://login.windows.net/5f3d9a6a-aaaa-aaaa-aaaa-aaaaaaaaaaaa/oauth2/token",
        "clientId": "1016c0cb-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "credentials": "f1cF7CwFJKlMWXPzAAAA1XB7BErAAAAAAAAAAAAAAAA=",
        "path": "dirToDelete/",
        "fileRegex": ".*\.csv",
        "continueOnError": "false"
        }
    }
