package com.aws.athena.udf.h3.infrastructure;

import java.util.Arrays;
import java.util.List;

import software.amazon.awscdk.core.BundlingOptions;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.DockerVolume;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.s3.assets.AssetOptions;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.FunctionProps;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.logs.RetentionDays;

import static java.util.Collections.singletonList;
import static software.amazon.awscdk.core.BundlingOutput.ARCHIVED;

public class AthenaUDFStack extends Stack {
    public AthenaUDFStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public AthenaUDFStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        List<String> udfPackagingCommand = Arrays.asList(
                "/bin/sh",
                "-c",
                "mvn clean package " +
                "&& cp /asset-input/target/aws-h3-athena-udf-1.0-SNAPSHOT.jar /asset-output/"
        );


        BundlingOptions.Builder builderOptions = BundlingOptions.builder()
                .command(udfPackagingCommand)
                .image(software.amazon.awscdk.services.lambda.Runtime.JAVA_11.getBundlingImage())
                .volumes(singletonList(
                        // Mount local .m2 repo to avoid download all the dependencies again inside the container
                        DockerVolume.builder()
                                .hostPath(System.getProperty("user.home") + "/.m2/")
                                .containerPath("/root/.m2/")
                                .build()
                ))
                .user("root")
                .outputType(ARCHIVED);

        Function  udf = new Function(this, "H3AthenaHandler", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .code(Code.fromAsset("../udf/", AssetOptions.builder()
                        .bundling(builderOptions
                                .command(udfPackagingCommand)
                                .build())
                        .build()))
                .handler("com.aws.athena.udf.h3.H3AthenaHandler")
                .memorySize(4096)
                .timeout(Duration.seconds(30))
                .logRetention(RetentionDays.ONE_WEEK)
                .build());
    }
}
