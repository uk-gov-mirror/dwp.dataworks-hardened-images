package uk.gov.dwp.dataworks.azkaban.jobtype;

import azkaban.Constants;
import azkaban.Constants.JobProperties;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.ExecuteAsUser;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import uk.gov.dwp.dataworks.lambdas.EMRConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_GROUP_NAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;


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
  public static final String AWS_LOG_GROUP_NAME = "aws.log.group.name";
  public static final String AWS_REGION = "aws.region";
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
  private static final String BOOT_POLL_INTERVAL = "emr.boot.poll.interval";
  private static final int BOOT_POLL_INTERVAL_DEFAULT = 300000; /* 5 mins */
  private static final String BOOT_POLL_ATTEMPTS_MAX = "emr.boot.poll.attempts.max";
  private static final int BOOT_POLL_ATTEMPTS_MAX_DEFAULT = 5;

  private final CommonMetrics commonMetrics;
  private volatile AzkabanProcess process;
  private volatile boolean killed = false;
  // For testing only. True if the job process exits successfully.
  private volatile boolean success;
  private volatile List<String> stepIds;

  public EMRStep(final String jobId, final Props sysProps,
      final Props jobProps, final Logger log) {
    super(jobId, sysProps, jobProps, log);
    // TODO: reallocf fully guicify CommonMetrics through ProcessJob dependents
    this.commonMetrics = SERVICE_PROVIDER.getInstance(CommonMetrics.class);
  }

  @Override
  public void run() throws Exception {
    info("Running EMR Step...");

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

    String awsRegion = this.getSysProps().getString(AWS_REGION, "eu-west-2");

    AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
			.withRegion(awsRegion)
			.build();

    String clusterId = getClusterId(emr);

    if (killed) {
        info("Job has been killed so exiting run");
        return;
    }

    configureCluster(emr, clusterId);

    ArrayList<String> args = new ArrayList<>(Arrays.asList(getUserGroup(effectiveUser)));
    args.addAll(retrieveScript(this.getJobProps().getString(COMMAND)));
    args.add(retrieveScriptArguments(this.getJobProps().getString(COMMAND)));

    StepFactory stepFactory = new StepFactory();

    StepConfig runBashScript = new StepConfig()
        .withName(this.getSysProps().getString(AWS_EMR_STEP_NAME)) 
        .withHadoopJarStep(new StepFactory(awsRegion + ".elasticmapreduce")
          .newScriptRunnerStep(this.getSysProps().getString(AWS_EMR_STEP_SCRIPT), args.toArray(new String[args.size()])))
        .withActionOnFailure("CONTINUE");

    AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest()
		  .withJobFlowId(clusterId)
		  .withSteps(runBashScript));

    this.stepIds = result.getStepIds();
    String stepId = this.stepIds.get(0);

    AWSLogsClient logsClient = new AWSLogsClient().withRegion(RegionUtils.getRegion(awsRegion));

    String logGroupName = this.getSysProps().getString(AWS_LOG_GROUP_NAME, "/aws/emr/azkaban");

    boolean stepCompleted = false;

    GetLogEventsRequest getLogEventsRequest = new GetLogEventsRequest()
      .withLogGroupName(logGroupName)
      .withLogStreamName(stepId)
      .withStartFromHead(true);

    info("Loop starting");

    while(! stepCompleted) {
      Thread.sleep(POLL_INTERVAL);

      if (killed) {
          info("Stopping waiting for step to complete due to job being killed");
          return;
      }

      Pair<Boolean, String> completionStatus = getStepStatus(emr, clusterId, stepId);
      stepCompleted = completionStatus.getFirst();

      if (stepCompleted && !completionStatus.getSecond().equals("COMPLETED")){
        error(String.format("Step %s did not successfully complete. Reason: %s", stepId, completionStatus.getSecond()));
        throw new RuntimeException(
                String.format("Step %s did not successfully complete. Reason: %s", stepId, completionStatus.getSecond())
        );
      }

      try {
        GetLogEventsResult logResult = logsClient.getLogEvents(getLogEventsRequest);
        printLogs(logResult);
        getLogEventsRequest = new GetLogEventsRequest()
          .withLogGroupName(logGroupName)
          .withLogStreamName(stepId)
          .withNextToken(logResult.getNextForwardToken());
      } catch(AWSLogsException e) {
        info("Waiting for logs to become available");
      }
    }

    // Get the output properties from this job.
    generateProperties(propFiles[1]);

    info("EMR Step Complete");
  }

  private void printLogs(GetLogEventsResult logResult) {
    for (OutputLogEvent event : logResult.getEvents()) {
      info(event.getMessage());
    }
  }

  private Pair<Boolean, String> getStepStatus(AmazonElasticMapReduce emr, String clusterId, String stepId) {
    ListStepsResult steps = emr.listSteps(new ListStepsRequest().withClusterId(clusterId));
    for (StepSummary step : steps.getSteps()) {
      if (step.getId().equals(stepId)) {
        return new Pair<>(
                (
                        step.getStatus().getState().equals("COMPLETED") ||
                        step.getStatus().getState().equals("CANCELLED") ||
                        step.getStatus().getState().equals("FAILED")
                ),
                step.getStatus().getState());
      }
    }
    error("Failed to find step with ID: " + stepId);
    return new Pair<>(false, "");
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
    boolean invokedLambda = false;

    int pollTime = this.getSysProps().getInt(BOOT_POLL_INTERVAL, BOOT_POLL_INTERVAL_DEFAULT);
    int maxAttempts = this.getSysProps().getInt(BOOT_POLL_ATTEMPTS_MAX, BOOT_POLL_ATTEMPTS_MAX_DEFAULT);
    String clusterName = this.getSysProps().getString(AWS_EMR_CLUSTER_NAME);
    while(!killed && clusterId == null && maxAttempts > 0) {
      ListClustersRequest clustersRequest = getClusterRequest();
      ListClustersResult clustersResult = emr.listClusters(clustersRequest);
      List<ClusterSummary> clusters = clustersResult.getClusters();
      for (ClusterSummary cluster : clusters) {
        if (cluster.getName().equals(clusterName)) {
          clusterId = cluster.getId();
        }
      }

      if (clusterId == null && !invokedLambda) {
        info("Starting up cluster");
        EMRConfiguration batchConfig = EMRConfiguration.builder().withName(clusterName).build();

        String payload = "{}";

        try {
          payload = new ObjectMapper().writeValueAsString(batchConfig);
        } catch (Exception e) {
          error(e.getMessage());
          throw new IllegalStateException(e);
        }

        AWSLambda client = AWSLambdaClientBuilder.defaultClient();
        InvokeRequest req = new InvokeRequest()
                  .withFunctionName("aws_analytical_env_emr_launcher")
                  .withPayload(payload);

        InvokeResult result = client.invoke(req);
        invokedLambda = true;

        if (result.getStatusCode() != 200) {
            error(result.getFunctionError());
            throw new IllegalStateException(result.getFunctionError());
        }
      }

      if (clusterId == null) {
        maxAttempts--;
        try {
          Thread.sleep(pollTime);
        } catch (Exception e) {
          warn("Sleep interrupted");
        }
      }
    }

    if (!killed && clusterId == null) {
      throw new IllegalStateException("No batch EMR cluster available");
    }

    return clusterId;
  }

  private ListClustersRequest getClusterRequest() {
    Collection<String> clusterStates = new ArrayList<String>();
    clusterStates.add("STARTING");
    clusterStates.add("BOOTSTRAPPING");
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
    set_job_to_killed();

    String awsRegion = this.getSysProps().getString(AWS_REGION, "eu-west-2");

    AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
			.withRegion(awsRegion)
			.build();

    String clusterId = null;
    try {
        clusterId = getClusterId(emr);
    } catch (IllegalStateException e) {
        info("No cluster found, killing job"); 
        kill_job();
        return;
    }

    if (clusterId == null) {
        info("Cluster not returned, killing job"); 
        kill_job();
        return;
    }
    info("Retrieved cluster with clusterId: " + clusterId);

    String stepId = this.stepIds.get(0);
    if (stepId == null) {
        info("No step found, killing job"); 
        kill_job();
        return;
    }
    info("Requesting step to cancel with stepId: " + stepId);

    ArrayList<String> steps = new ArrayList<String>();
    steps.add(stepId);

    try {
        emr.cancelSteps(getCancelStepsRequest(clusterId, steps));
    } catch (IllegalStateException e) {
        info("Error occurred killing job"); 
        kill_job();
        return;
    }

    info("EMR step requested to cancel");
    kill_job();
  }

  private CancelStepsRequest getCancelStepsRequest(String clusterId, Collection<String> stepIds) {
    CancelStepsRequest request = new CancelStepsRequest();
    request.setClusterId(clusterId);
    request.setStepIds(stepIds);
    return request;
  }

  private void set_job_to_killed() throws InterruptedException  {
    synchronized (this) {
        this.killed = true;
        this.notify();
        if (this.process == null) {
            return;
        }
    }
  }

  private void kill_job() throws InterruptedException  {
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
