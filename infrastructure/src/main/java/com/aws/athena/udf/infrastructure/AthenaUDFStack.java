package com.aws.athena.udf.h3.infrastructure;

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

import java.util.Arrays;

/** CDK Stack for H3 UDF. */
public class AthenaUDFStack extends Stack {
    /** The memory size to be used by the lambda. */    
    private static final int MEMORY_SIZE = 4096;

    /** The timeout of lambda execution. */
    private static final int TIMEOUT = 30;

    public AthenaUDFStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public AthenaUDFStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // The packaging command to create the jar file of the UDF
        final List<String> udfPkgCommand = Arrays.asList(
                "/bin/sh",
                "-c",
                "cd udf" + 
                "&& mvn clean install  -Dmaven.test.skip=true" +
                "&& cp target/aws-h3-athena-udf*.jar /asset-output/"
        );
        final BundlingOptions.Builder builderOptions = BundlingOptions.builder()
                .command(udfPkgCommand)
                .image(software.amazon.awscdk.services.lambda.Runtime.JAVA_8.getBundlingImage())
                .volumes(singletonList(
                        // Mount local .m2 repo to avoid download all the dependencies again inside the container
                        DockerVolume.builder()
                                .hostPath(System.getProperty("user.home") + "/.m2/")
                                .containerPath("/root/.m2/")
                                .build()
                ))
                .user("root")
                .outputType(ARCHIVED);


        // Creates the UDF.
        final Function  udf = new Function(this, "H3AthenaHandler", FunctionProps.builder()
                .runtime(Runtime.JAVA_8)
                .code(Code.fromAsset("../", AssetOptions.builder()
                        .bundling(builderOptions
                                .command(udfPkgCommand)
                                .build())
                        .build()))
                .handler("com.aws.athena.udf.h3.H3AthenaHandler")
                .memorySize(MEMORY_SIZE)
                .timeout(Duration.seconds(TIMEOUT))
                .logRetention(RetentionDays.ONE_WEEK)
                .build());
    }
}
