package io.cdap.plugin.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * ADLS delete action plugin to delete files on adls
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("ADLSDelete")
@Description("Action to delete files on ADLS")
public class ADLSDeleteAction extends Action {

  private static final Logger LOG = LoggerFactory.getLogger(ADLSDeleteAction.class);

  private ADLSDeleteActionConfig config;
  private PathFilter filter;

  public ADLSDeleteAction(ADLSDeleteActionConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path path = new Path(config.path);

    Configuration conf = new Configuration();

    Map<String, String> properties = getFileSystemProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    FileSystem fileSystem = FileSystem.get(conf);

    // delete the path, if its a directory without regex or a file
    if (fileSystem.isFile(path) || (fileSystem.isDirectory(path) && config.fileRegex == null)) {
      removePath(fileSystem, path);
      return;
    }

    // if its a directory and regex is specified then list and delete all the matching files
    if (config.fileRegex != null) {
      PathFilter filter = new PathFilter() {
        private final Pattern pattern = Pattern.compile(config.fileRegex);

        @Override
        public boolean accept(Path path) {
          return pattern.matcher(path.getName()).matches();
        }
      };

      FileStatus[] listFiles = fileSystem.listStatus(path, filter);

      for (FileStatus file : listFiles) {
        Path currPath = file.getPath();
        LOG.debug("deleting file: {}", currPath.toString());
        removePath(fileSystem, currPath);
      }
    }
  }

  public void removePath(FileSystem fileSystem, Path currPath) throws Exception {
    try {
      if (!fileSystem.delete(currPath, true)) {
        if (!config.continueOnError) {
          throw new IOException(String.format("Removal of %s was unsuccessful.", currPath.toString()));
        }
        LOG.warn("Removal of {} was unsuccessful.", currPath.toString());
      }
    } catch (IOException e) {
      if (!config.continueOnError) {
        throw e;
      }
      LOG.warn("Removal of {} was unsuccessful.", currPath.toString());
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (config.fileRegex != null) {
      try {
        Pattern.compile(config.fileRegex);
      } catch (Exception e) {
        pipelineConfigurer.getStageConfigurer().getFailureCollector()
          .addFailure(String.format("File regex '%s' is invalid: %s", config.fileRegex, e.getMessage()), null)
          .withConfigProperty(ADLSDeleteActionConfig.FILE_REGEX);
      }
    }
  }

  /**
   * Config class that contains all properties necessary to execute an ADLS delete command.
   */
  public class ADLSDeleteActionConfig extends PluginConfig {
    private static final String FILE_REGEX = "fileRegex";
    @Description("The full ADLS path of the file or files that need to be deleted. If path points to a file, " +
      "the file will be removed. If path points to a directory with no regex specified, the directory and all of " +
      "its contents will be removed. If a regex is specified, only the files and directories matching that regex " +
      "will be removed")
    @Macro
    private String path;

    @Description("Regular expression to filter the files in the source directory that will be deleted")
    @Nullable
    @Macro
    private String fileRegex;

    @Description("Indicates if the pipeline should continue if the delete fails")
    private boolean continueOnError;

    @Description("ADLS File system uri")
    @Macro
    private String fileSystemUri;

    @Description("ADLS refresh token URL")
    @Macro
    private String refreshTokenURL;

    @Description("ADLS client id")
    @Macro
    private String clientId;

    @Description("ADLS credentials")
    @Macro
    private String credentials;

    @VisibleForTesting
    ADLSDeleteActionConfig(String path, String fileRegex, boolean continueOnError, String fileSystemUri,
                           String refreshTokenURL, String clientId, String credentials) {
      this.path = path;
      this.fileRegex = fileRegex;
      this.continueOnError = continueOnError;
      this.fileSystemUri = fileSystemUri;
      this.refreshTokenURL = refreshTokenURL;
      this.clientId = clientId;
      this.credentials = credentials;
    }
  }

  private Map<String, String> getFileSystemProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("fs.defaultFS", config.fileSystemUri);
    properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
    properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
    properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");
    properties.put("dfs.adls.oauth2.refresh.url", config.refreshTokenURL);
    properties.put("dfs.adls.oauth2.client.id", config.clientId);
    properties.put("dfs.adls.oauth2.credential", config.credentials);
    return properties;
  }
}
