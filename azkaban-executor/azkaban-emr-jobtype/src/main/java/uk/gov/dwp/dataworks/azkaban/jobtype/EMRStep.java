package uk.gov.dwp.dataworks.azkaban.jobtype;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_GROUP_NAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.Constants;
import azkaban.Constants.JobProperties;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.ExecuteAsUser;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.SystemMemoryInfo;
import azkaban.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.io.File;
import java.nio.file.Files;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.Scanner;
import java.util.Collection;
import org.apache.log4j.Logger;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.regions.Regions;


/**
 * A job that runs an AWS EMR step
 */
public class EMRStep extends AbstractProcessJob {

  public static final String COMMAND = "step";
  public static final String AWS_EMR_CLUSTER_NAME = "aws.emr.cluster.name";
  public static final String AWS_EMR_CLUSTER_STATEFILE = "aws.emr.cluster.statefile";
  public static final String AWS_EMR_STEP_SCRIPT = "aws.emr.step.script";
  public static final String AWS_EMR_STEP_NAME = "aws.emr.step.name";
  public static final String AZKABAN_MEMORY_CHECK = "azkaban.memory.check";
  // Use azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER instead
  @Deprecated
  public static final String NATIVE_LIB_FOLDER = "azkaban.native.lib";
  public static final String EXECUTE_AS_USER = "execute.as.user";
  public static final String KRB5CCNAME = "KRB5CCNAME";
  private static final Duration KILL_TIME = Duration.ofSeconds(30);
  private static final String MEMCHECK_ENABLED = "memCheck.enabled";
  private static final String CHOWN = "/bin/chown";
  private static final String CREATE_FILE = "touch";
  private static final int SUCCESSFUL_EXECUTION = 0;
  private static final String TEMP_FILE_NAME = "user_can_write";
  private static final int MAX_STEPS = 256;
  private static final int POLL_INTERVAL = 10000;

  private final CommonMetrics commonMetrics;
  private volatile AzkabanProcess process;
  private volatile boolean killed = false;
  // For testing only. True if the job process exits successfully.
  private volatile boolean success;

  public EMRStep(final String jobId, final Props sysProps,
      final Props jobProps, final Logger log) {
    super(jobId, sysProps, jobProps, log);
    // TODO: reallocf fully guicify CommonMetrics through ProcessJob dependents
    this.commonMetrics = SERVICE_PROVIDER.getInstance(CommonMetrics.class);
  }

  @Override
  public void run() throws Exception {
    try {
      resolveProps();
    } catch (final Exception e) {
      handleError("Bad property definition! " + e.getMessage(), e);
    }

    final File[] propFiles = initPropsFiles();

    // determine whether to run as Azkaban or run as effectiveUser,
    // by default, run as effectiveUser
    String executeAsUserBinaryPath = null;
    String effectiveUser = null;
    final boolean isExecuteAsUser = this.getSysProps().getBoolean(EXECUTE_AS_USER, true);

    //Get list of users we never execute flows as. (ie: root, azkaban)
    final Set<String> blackListedUsers = new HashSet<>(
        Arrays.asList(
            this.getSysProps()
                .getString(Constants.ConfigurationKeys.BLACK_LISTED_USERS, "root,azkaban")
                .split(",")
        )
    );

    // nativeLibFolder specifies the path for execute-as-user file,
    // which will change user from Azkaban to effectiveUser
    if (isExecuteAsUser) {
      final String nativeLibFolder = this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER);
      executeAsUserBinaryPath = String.format("%s/%s", nativeLibFolder, "execute-as-user");
      effectiveUser = getEffectiveUser(this.getJobProps());
      // Throw exception if Azkaban tries to run flow as a prohibited user
      if (blackListedUsers.contains(effectiveUser)) {
        throw new RuntimeException(
            String.format("Not permitted to proxy as '%s' through Azkaban", effectiveUser)
        );
      }
      // Set parent directory permissions to <uid>:azkaban so user can write in their execution directory
      // if the directory is not permissioned correctly already (should happen once per execution)
      if (!canWriteInCurrentWorkingDirectory(effectiveUser)) {
        info("Changing current working directory ownership");
        assignUserFileOwnership(effectiveUser, getWorkingDirectory());
      }
      // Set property file permissions to <uid>:azkaban so user can write to their prop files
      // in order to pass properties from one job to another, except the last one
      for (int i = 0; i < 2; i++) {
        info("Changing properties files ownership");
        assignUserFileOwnership(effectiveUser, propFiles[i].getAbsolutePath());
      }
    }

    this.logJobProperties();

    AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
			.withRegion(Regions.EU_WEST_2)
			.build();

    String clusterId = getClusterId(emr);

    configureCluster(emr, clusterId);

    ArrayList<String> args = retrieveScript(this.getJobProps().getString(COMMAND));
    args.add(getUserGroup(effectiveUser));
    args.add(retrieveScriptArguments(this.getJobProps().getString(COMMAND)));

    StepFactory stepFactory = new StepFactory();

    StepConfig runBashScript = new StepConfig()
        .withName(this.getSysProps().getString(AWS_EMR_STEP_NAME)) 
        .withHadoopJarStep(new StepFactory("eu-west-2.elasticmapreduce")
          .newScriptRunnerStep(this.getSysProps().getString(AWS_EMR_STEP_SCRIPT), args.toArray(new String[args.size()])))
        .withActionOnFailure("CONTINUE");

    AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest()
		  .withJobFlowId(clusterId)
		  .withSteps(runBashScript));

    // Get the output properties from this job.
    generateProperties(propFiles[1]);
  }

  private String retrieveScriptArguments(String command) {
    if (command.contains(" ")) {
      String[] parts = command.split(" ");
      String[] scriptArgsArray = Arrays.copyOfRange(parts, 1, parts.length);
      return String.join(" ", scriptArgsArray);
    } else { 
      return "";
    }
  }

  private ArrayList<String> retrieveScript(String command) {
    if (command.contains(" ")) {
      return new ArrayList<String>(Arrays.asList(command.split(" ")[0]));
    } else {
      return new ArrayList<String>(Arrays.asList(command));
    }
  }

  protected String getClusterId(AmazonElasticMapReduce emr) {
    String clusterId = null;
    ListClustersRequest clustersRequest = getClusterRequest();
    ListClustersResult clustersResult = emr.listClusters(clustersRequest);
    List<ClusterSummary> clusters = clustersResult.getClusters();
    for (ClusterSummary cluster : clusters) {
      if (cluster.getName().equals(this.getSysProps().getString(AWS_EMR_CLUSTER_NAME))) {
        clusterId = cluster.getId();
      }
    }
    return clusterId;
  }

  private ListClustersRequest getClusterRequest() {
    Collection<String> clusterStates = new ArrayList<String>();
    clusterStates.add("WAITING");
    clusterStates.add("RUNNING");
    ListClustersRequest clustersRequest = new ListClustersRequest();
    clustersRequest.setClusterStates(clusterStates);
    return clustersRequest;
  }

  /**
   * <pre>
   * Determines what user id should the process job run as, in the following order of precedence:
   * 1. USER_TO_PROXY
   * 2. SUBMIT_USER
   * </pre>
   *
   * @return the user that Azkaban is going to execute as
   */
  private String getEffectiveUser(final Props jobProps) {
    String effectiveUser = null;
    if (jobProps.containsKey(JobProperties.USER_TO_PROXY)) {
      effectiveUser = jobProps.getString(JobProperties.USER_TO_PROXY);
    } else if (jobProps.containsKey(CommonJobProperties.SUBMIT_USER)) {
      effectiveUser = jobProps.getString(CommonJobProperties.SUBMIT_USER);
    } else {
      throw new RuntimeException(
          "Internal Error: No user.to.proxy or submit.user in the jobProps");
    }
    info("effective user is: " + effectiveUser);
    return effectiveUser;
  }

  /**
   * Checks to see if user has write access to current working directory which many users need for
   * their jobs to store temporary data/jars on the executor.
   *
   * Accomplishes this by using execute-as-user to try to create an empty file in the cwd.
   *
   * @param effectiveUser user/proxy user running the job
   * @return true if user has write permissions in current working directory otherwise false
   */
  private boolean canWriteInCurrentWorkingDirectory(final String effectiveUser)
      throws IOException {
    final ExecuteAsUser executeAsUser = new ExecuteAsUser(
        this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
    final List<String> checkIfUserCanWriteCommand = Arrays
        .asList(CREATE_FILE, getWorkingDirectory() + "/" + TEMP_FILE_NAME);
    final int result = executeAsUser.execute(effectiveUser, checkIfUserCanWriteCommand);
    return result == SUCCESSFUL_EXECUTION;
  }

  /**
   * Changes permissions on file/directory so that the file/directory is owned by the user and the
   * group remains the azkaban service account name.
   *
   * Leverages execute-as-user with "root" as the user to run the command.
   *
   * @param effectiveUser user/proxy user running the job
   * @param fileName the name of the file whose permissions will be changed
   */
  private void assignUserFileOwnership(final String effectiveUser, final String fileName) throws
      Exception {
    final ExecuteAsUser executeAsUser = new ExecuteAsUser(
        this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
    final String groupName = this.getSysProps().getString(AZKABAN_SERVER_GROUP_NAME, "azkaban");
    final List<String> changeOwnershipCommand = Arrays
        .asList(CHOWN, effectiveUser + ":" + groupName, fileName);
    info("Change ownership of " + fileName + " to " + effectiveUser + ":" + groupName + ".");
    final int result = executeAsUser.execute("root", changeOwnershipCommand);
    if (result != 0) {
      handleError("Failed to change current working directory ownership. Error code: " + Integer
          .toString(result), null);
    }
  }

  /**
   * This is used to get the min/max memory size requirement by processes. SystemMemoryInfo can use
   * the info to determine if the memory request can be fulfilled. For Java process, this should be
   * Xms/Xmx setting.
   *
   * @return pair of min/max memory size
   */
  protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
    return new Pair<>(0L, 0L);
  }

  protected void handleError(final String errorMsg, final Exception e) throws Exception {
    error(errorMsg);
    if (e != null) {
      throw new Exception(errorMsg, e);
    } else {
      throw new Exception(errorMsg);
    }
  }

  protected List<String> getCommandList() {
    final List<String> commands = new ArrayList<>();
    commands.add(this.getJobProps().getString(COMMAND));
    for (int i = 1; this.getJobProps().containsKey(COMMAND + "." + i); i++) {
      commands.add(this.getJobProps().getString(COMMAND + "." + i));
    }

    return commands;
  }

  private String getUserGroup(String user) {
    String groupName = null;
    FileReader fileReader = null;
    BufferedReader groupsReader = null;
    try {
      fileReader = new FileReader("/etc/group");
      groupsReader = new BufferedReader(fileReader);
      while(groupsReader.ready()) {
        try {
          String line = groupsReader.readLine();
          String [] tokens = line.split(":");
          if(tokens.length > 3) {
            for(String uStr: tokens[3].split(",")) {
              if (uStr.equals(user)) {
                groupName = tokens[0];
                break;
              }
            }
          }
        } catch (Exception e) { 
          continue; 
        }
      }
      groupsReader.close();
      fileReader.close();
    } catch (Exception e) {
      return groupName;
    }
    return groupName;
  }

  @Override
  public void cancel() throws InterruptedException {
    // in case the job is waiting
    synchronized (this) {
      this.killed = true;
      this.notify();
      if (this.process == null) {
        // The job thread has not checked if the job is killed yet.
        // setting the killed flag should be enough to abort the job.
        // There is no job process to kill.
        return;
      }
    }
    this.process.awaitStartup();
    final boolean processkilled = this.process
        .softKill(KILL_TIME.toMillis(), TimeUnit.MILLISECONDS);
    if (!processkilled) {
      warn("Kill with signal TERM failed. Killing with KILL signal.");
      this.process.hardKill();
    }
  }

  @Override
  public double getProgress() {
    return this.process != null && this.process.isComplete() ? 1.0 : 0.0;
  }

  public int getProcessId() {
    return this.process.getProcessId();
  }

  public String getPath() {
    return Utils.ifNull(this.getJobPath(), "");
  }

  private void configureCluster(AmazonElasticMapReduce emr, String clusterId) {
    DescribeClusterResult clusterDetails = emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
    if (clusterDetails.getCluster().getStepConcurrencyLevel() != MAX_STEPS) {
      // Post Startup Implementation details here.
      // We may still be initializing, so wait until we are waiting for steps.
      // Potentially maybe other executors listening, so check step concurrency also.
      while(! clusterDetails.getCluster().getStatus().getState().equals("WAITING")) {
        info("Waiting for state WAITING...current state is " + clusterDetails.getCluster().getStatus().getState());
        try {
          Thread.sleep(POLL_INTERVAL);
        } catch (Exception e) {
          warn("Sleep interrupted");
        }
        clusterDetails = emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId));
      }

      if (clusterDetails.getCluster().getStepConcurrencyLevel() != MAX_STEPS) {
        ModifyClusterResult modifyResult = emr.modifyCluster(new ModifyClusterRequest().withClusterId(clusterId)
                .withStepConcurrencyLevel(MAX_STEPS));
        // Add additional setup here.
      }
    }
  }
}
