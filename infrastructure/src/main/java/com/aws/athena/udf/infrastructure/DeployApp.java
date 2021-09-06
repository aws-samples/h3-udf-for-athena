package  com.aws.athena.udf.h3.infrastructure;

import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Environment;
import software.amazon.awscdk.core.StackProps;

import java.util.Arrays;

public class DeployApp {
    public static void main(final String[] args) {
        App app = new App();

        new AthenaUDFStack(app, "AthenaUDFStack", StackProps.builder()
                .env(Environment.builder()
                        .account("705240738422")
                        .region("eu-west-1")
                        .build())
                .build());

        app.synth();
    }
}
