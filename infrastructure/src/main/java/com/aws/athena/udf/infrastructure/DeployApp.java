package  com.aws.athena.udf.h3.infrastructure;

import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Environment;
import software.amazon.awscdk.core.StackProps;

import java.util.Arrays;

/** The main application to deploy UDF stack using CDK. 
 *  The code assumes that the account and region are available in your environment. 
 *  
 */
public class DeployApp {
    public static void main(final String[] args) {
        App app = new App();

        new AthenaUDFStack(app, "AthenaUDFStack", StackProps.builder()
                .env(Environment.builder().build())
                .build());

        app.synth();
    }
}
