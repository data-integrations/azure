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
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.azure.keyvault.models.SecretBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//for jceks
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;

public class AzureClientSecretService {

  private static Logger logger = LoggerFactory.getLogger(AzureClientSecretService.class);

  public static HashMap<String, String> getADLSSecretsUsingJceksAndKV(String keyVaultUrl, HashMap<String, String> credMap) {
    /* First get the credentials to access KeyVault from Jceks file */
    HashMap<String, String> keymap = new HashMap<String, String>();
    try {
      keymap = getSecretsFromJcek("fs.adl.oauth2.client.id,fs.adl.oauth2.credential"); 
      logger.info("KeyVault access identified");
    } catch (Exception e) {
      logger.error("jcek Exception : " + e);
    }
    return getSecretsFromKV(keyVaultUrl, credMap, keymap.get("fs.adl.oauth2.client.id"), keymap.get("fs.adl.oauth2.credential"));
  }

  public static HashMap<String, String> getSecretsFromKV(String vaultURI, HashMap<String, String> secretMap, String kvId, String kvSecret) {

    /* Get keyVault client by providing authorized credentials as read from jceks */
    KeyVaultClient client = new KeyVaultClient(new ClientSecretKeyVaultCredential(kvId, kvSecret));
    
    /* Now, Replace value in secretMap with actual value fetched from keyVault */
    for (String key : secretMap.keySet()) {
      SecretBundle secret = client.getSecret(vaultURI, secretMap.get(key));
      secretMap.put(key,secret.value());
    }
    return secretMap;
  }

  public static HashMap<String, String> getSecretsFromJcek(String csvKeys) throws IOException {

    HashMap<String, String> keyPasses = new HashMap<String, String>();
    String[] keys = csvKeys.split(",");

    /* Fetch password from configured credential provider path */
    Configuration c = new Configuration();
    /* TODO: jceks file location hardcoded for now, it is to be replaced by some other strategy like either reading from
     * core-site.xml or cdap environment, etc. Till this story is defined, we keep it hard-coded
     */
    c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, "jceks://hdfs@mycluster/etc/security/jceks/adls.jceks");
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
}

