# Microsoft Azure Blob Storage Batch Source

Description
-----------

Batch source to read from Microsoft Azure Blob Storage (WASB) or Azure Data Lake Storage Gen2 (ABFS).

Use Case
--------

This source is used whenever you need to read files from Azure Blob Storage or Azure Data Lake Storage Gen2.
For example, you may want to read log files from Azure every hour and store the results in a database or
another storage system.

Supported Path Schemes
----------------------

| Scheme | Driver | Typical Use |
|--------|--------|-------------|
| `wasb://` | NativeAzureFileSystem (WASB) | Azure Blob Storage |
| `wasbs://` | NativeAzureFileSystem (WASB, SSL) | Azure Blob Storage (secure) |
| `abfs://` | AzureBlobFileSystem (ABFS) | Azure Data Lake Storage Gen2 |
| `abfss://` | SecureAzureBlobFileSystem (ABFS, SSL) | Azure Data Lake Storage Gen2 (secure) |

Supported Authentication Methods
---------------------------------

| Method | Works with | Credentials required |
|--------|-----------|----------------------|
| Storage Account Key | wasb, wasbs, abfs, abfss | Storage key |
| SAS Token | wasb, wasbs | SAS token + container |
| Service Principal | abfs, abfss | Tenant ID, Client ID, Client secret |
| Managed Identity | abfs, abfss | None (identity from Azure runtime) |

Properties
----------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Path:** The path to use as input. Uses filename expansion (globbing) to read files. The path scheme
determines which driver and authentication methods are available. (Macro-enabled)

- WASB example: `wasb://mycontainer@myaccount.blob.core.windows.net/data/`
- ABFS example: `abfss://mycontainer@myaccount.dfs.core.windows.net/data/`

**Account:** The Microsoft Azure Storage account FQDN. (Macro-enabled)

- For `wasb://` / `wasbs://` paths, must end with `.blob.core.windows.net`
  (e.g. `mystorageaccount.blob.core.windows.net`).
- For `abfs://` / `abfss://` paths, must end with `.dfs.core.windows.net`
  (e.g. `mystorageaccount.dfs.core.windows.net`).

**Authentication Method:** The method used to authenticate with Azure. Defaults to `Storage Account Key`.

---

### Storage Account Key

**Storage Key:** The base64-encoded storage key for the Azure Storage account.
Required when authentication method is `Storage Account Key`. (Macro-enabled)

---

### SAS Token

SAS Token authentication is supported for `wasb://` and `wasbs://` paths only.

**SAS Token:** The Shared Access Signature token for the container.
Required when authentication method is `SAS Token`. (Macro-enabled)

**Container:** The blob container to connect to.
Required when authentication method is `SAS Token`. (Macro-enabled)

---

### Service Principal (Azure AD)

Service Principal authentication uses Azure Active Directory OAuth2 client credentials.
Requires `abfs://` or `abfss://` paths.

**Tenant ID:** The Azure Active Directory tenant (directory) ID.
Required when authentication method is `Service Principal`. (Macro-enabled)

**Client ID:** The application (client) ID of the registered Azure AD service principal.
Required when authentication method is `Service Principal`. (Macro-enabled)

**Client Secret:** The client secret value generated for the Azure AD service principal.
Required when authentication method is `Service Principal`. (Macro-enabled)

> The service principal must be assigned the **Storage Blob Data Reader** role (or equivalent)
> on the storage account or container in Azure IAM.

---

### Managed Identity (Azure AD)

Managed Identity authentication uses the Azure-assigned identity of the compute resource running
the pipeline (e.g. an Azure VM, AKS node pool, or HDInsight cluster). No credentials are required.
Requires `abfs://` or `abfss://` paths.

> The managed identity must be assigned the **Storage Blob Data Reader** role (or equivalent)
> on the storage account or container in Azure IAM.

---

**Format:** Format of the data to read. The format must be one of `avro`, `blob`, `csv`, `delimited`,
`json`, `parquet`, `text`, `tsv`, or `xls`.
- `blob`: Every input file is read into a separate record. Requires a schema with a field named `body` of type `bytes`.
- `text`: Files are read line by line. Requires a schema with a field named `body` of type `string`.
- `csv`: Comma-separated values. Supports header row and quoted values.
- `tsv`: Tab-separated values. Supports header row and quoted values.
- `delimited`: Delimiter-separated values using a configurable delimiter.
- `avro`: Apache Avro binary format. Schema is read from the file.
- `parquet`: Apache Parquet columnar format. Schema is read from the file.
- `json`: JSON objects, one per line.
- `xls`: Microsoft Excel spreadsheet format.

**Sample Size:** The maximum number of rows investigated for automatic data type detection.
Default is 1000. Only used when format is `xls`.

**Override:** Per-column data type overrides that skip automatic type detection.
Only used when format is `xls`.

**Delimiter:** The delimiter to use when format is `delimited`. Ignored for other formats.

**Enable Quoted Values:** Whether to treat content between quotes as a value. Applies to `csv`, `tsv`,
and `delimited` formats.

**Use First Row as Header:** Whether to use the first row as a header. Applies to `csv`, `tsv`,
`delimited`, and `xls` formats.

**Terminate Reading After Empty Row:** Stop reading after the first empty row. Defaults to false.
Only used when format is `xls`.

**Select Sheet Using / Sheet Value:** Sheet selection by name or number (0-based). Only used when format is `xls`.

**Maximum Split Size:** Maximum size in bytes for each input partition. Default is 128MB.

**Regex Path Filter:** Regular expression that file paths must match to be included in the input.

**Path Field:** Output field to store the file path each record was read from.
Must be a string field in the output schema.

**Path Filename Only:** Whether to use only the filename instead of the full URI in the path field.
Default is false.

**Read Files Recursively:** Whether to read files recursively from the path. Default is false.

**Allow Empty Input:** Whether to allow an input path with no data. When false, the plugin errors if
there is no data. Default is false.

**File System Properties:** Additional Hadoop filesystem properties as a JSON object. (Macro-enabled)

**File Encoding:** Character encoding for the file(s). Default is UTF-8.

Examples
--------

### Storage Account Key (WASB)

    {
        "name": "AzureBlobStore",
        "type": "batchsource",
        "properties": {
            "referenceName": "azure_blob_input",
            "path": "wasb://mycontainer@myaccount.blob.core.windows.net/data/",
            "account": "myaccount.blob.core.windows.net",
            "authenticationMethod": "storageAccountKey",
            "storageKey": "XXXXXEEESSS/YYYY=",
            "format": "csv",
            "skipHeader": "true"
        }
    }

### Service Principal (ABFS)

    {
        "name": "AzureBlobStore",
        "type": "batchsource",
        "properties": {
            "referenceName": "adls_gen2_input",
            "path": "abfss://mycontainer@myaccount.dfs.core.windows.net/data/",
            "account": "myaccount.dfs.core.windows.net",
            "authenticationMethod": "servicePrincipal",
            "tenantId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "clientId": "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
            "clientSecret": "your-client-secret",
            "format": "parquet",
            "recursiveRead": "true"
        }
    }

### Managed Identity (ABFS)

    {
        "name": "AzureBlobStore",
        "type": "batchsource",
        "properties": {
            "referenceName": "adls_gen2_input",
            "path": "abfss://mycontainer@myaccount.dfs.core.windows.net/data/",
            "account": "myaccount.dfs.core.windows.net",
            "authenticationMethod": "managedIdentity",
            "format": "json"
        }
    }
