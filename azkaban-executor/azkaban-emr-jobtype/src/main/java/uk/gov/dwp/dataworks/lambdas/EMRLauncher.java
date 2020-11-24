package uk.gov.dwp.dataworks.lambdas;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

public interface EMRLauncher {
    @LambdaFunction(functionName="aws_analytical_env_emr_launcher")
    void LaunchBatchEMR(EMRConfiguration config);
}
