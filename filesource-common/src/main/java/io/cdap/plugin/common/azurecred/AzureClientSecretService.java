/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.common.azurecred;


import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.models.SecretBundle;
import io.netty.util.internal.StringUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AzureClientSecretService {

  private static Logger logger = LoggerFactory.getLogger(AzureClientSecretService.class);

  /* To read ADLS credentials stored in KeyVault, we need to first access KeyVault Store itself
   * 1. Either user has provided KeyVault credentials itself in properties : `fs.adl.oauth2.client.id` and `fs.adl.oauth2.credential`
   * 2. If not, these credentials are encrypted in a hadoop's jceks file.
   * 3. jceks file path is either specified by user in property - hadoop.security.credential.provider.path
   * 4. If not, we should get this property value from core-site.xml
   * 5. If path not provided in core-site.xml also, we use the default jceks path on an azure vm
   */
  public static HashMap<String, String> getADLSSecretsUsingJceksAndKV(String keyVaultUrl, HashMap<String, String> credMap,
                                                                      Map<String, String> properties) {
    Map<String, String> keymap = properties;

    if (!(keymap.containsKey("fs.adl.oauth2.client.id") && keymap.containsKey("fs.adl.oauth2.credential"))) {
      // First get the credentials to access KeyVault from Jceks file
      try {
        keymap = getSecretsFromJcek("fs.adl.oauth2.client.id,fs.adl.oauth2.credential",
                                            properties.getOrDefault("hadoop.security.credential.provider.path", getJceksPath()));
        logger.info("KeyVault access identified");
      } catch (Exception e) {
        logger.error("jcek Exception : " + e);
      }
    }
    return getSecretsFromKV(keyVaultUrl, credMap, keymap.get("fs.adl.oauth2.client.id"), keymap.get("fs.adl.oauth2.credential"));
  }


  public static String getJceksPath() {

      String jceks = "jceks://hdfs@mycluster/etc/security/jceks/adls.jceks";  // init with default for Azure

      try {
          Configuration conf = new Configuration();
          // Assumption: this path is always correct in Azure for hdfs-site.xml and core-site.xml
          Path coresite = new Path("file:///etc/hadoop/conf/hdfs-site.xml");
          Path hdfssite = new Path("file:///etc/hadoop/conf/core-site.xml");
          conf.addResource(coresite);
          conf.addResource(hdfssite);
          FileSystem fs = coresite.getFileSystem(conf);
          String jpath = fs.getConf().get("hadoop.security.credential.provider.path","jceks://hdfs@mycluster/etc/security/jceks/adls.jceks");
          String ns = fs.getConf().get("dfs.internal.nameservices","mycluster");
          logger.info("jpath : " + jpath + "; ns : " + ns);
          ns = ns.split(",")[0].trim();     //pick the first nameservice string specified; all are valid ones
          fs.close();

          if (!StringUtil.isNullOrEmpty(jpath)) {
              // jpath list is found in core-site.xml but there could be multiple jceks present in this list and we are looking for one with name adls.jceks
              for (String ss : jpath.split(",")) {
                  // there could be multiple paths present, lets try picking the first adls.jceks path
                  String s = ss.trim();
                  String adlsJceks = s.substring(s.lastIndexOf("/")+1).trim();
                  logger.debug("inspecting for jpath : " + ss);
                  if (true == adlsJceks.equalsIgnoreCase("adls.jceks")) {
                      /* We need to insert the hdfs nameservice and fully qualify the hdfs path
                       */
                      if (s.startsWith("jceks://file/") || s.startsWith("localjceks://file/")
                          || s.startsWith("jceks://hdfs@") || s.startsWith("localjceks://hdfs@")
                          || (StringUtil.isNullOrEmpty(ns) && (s.startsWith("jceks://hdfs") || s.startsWith("localjceks://hdfs"))) ) {
                          /* Case1: local jceks file case or running on local sandbox case
                           * Case2: already a fully qualified hdfs path with nameservice
                           * Case3: nameservice not present(non-HA case) and path is already hdfs qualified
                           * in all above cases, use the path as specified, nothing more to do
                           */
                          jceks = s;
                      } else if (!StringUtil.isNullOrEmpty(ns) 
                                && (s.startsWith("jceks://hdfs") || s.startsWith("localjceks://hdfs")) ) {
                              // insert nameservice for already qualified hdfs path
                              jceks = s.split("hdfs")[0] + "hdfs@" + ns + "/" + s.split("hdfs")[1];
                      } else {
                          if (StringUtil.isNullOrEmpty(ns)) {
                              // nameservice not present
                              jceks = "localjceks://hdfs/" + s;   // not supporting jceks/user scheme type for now
                          } else {
                              jceks = "localjceks://hdfs@" + ns + "/" + s;
                          }
                      }
                      break;    // give priority to first adls.jceks file found
                  }
              }
          }
          logger.info("returning jceks path : " + jceks);
      } catch (Exception e) {
          logger.error("Exception caught while trying to read jceks file : " + e.getMessage());
      }
      return jceks;
  }


  public static HashMap<String, String> getSecretsFromJcek(String csvKeys, String jceksPath) throws IOException {
    HashMap<String, String> keyPasses = new HashMap<String, String>();
    String[] keys = csvKeys.split(",");
    logger.debug("jceksPath : " + jceksPath + "; csvKeys : " + csvKeys);

    // Fetch password from configured credential provider path
    Configuration c = new Configuration();
    c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksPath);
    CredentialProvider credentialProvider = CredentialProviderFactory.getProviders(c).get(0);
    for(String key : keys) {
      CredentialProvider.CredentialEntry entry = credentialProvider.getCredentialEntry(key);
      if (entry == null) {
        throw new IOException(String.format("No credential entry found for %s", key));
      } else {
        keyPasses.put(key, String.valueOf(entry.getCredential()));
      }
    }
    return keyPasses;
  }

  public static HashMap<String, String> getSecretsFromKV(String vaultURI, HashMap<String, String> secretMap, String kvId, String kvSecret) {
      // Get keyVault client by providing authorized credentials as read from jceks
      KeyVaultClient client = new KeyVaultClient(new ClientSecretKeyVaultCredential(kvId, kvSecret));

      // Now, Replace value in secretMap with actual value fetched from keyVault
      for (String key : secretMap.keySet()) {
          SecretBundle secret = client.getSecret(vaultURI, secretMap.get(key));
          secretMap.put(key,secret.value());
      }
      return secretMap;
  }
}

