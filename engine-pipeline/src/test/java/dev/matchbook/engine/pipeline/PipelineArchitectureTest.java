package dev.matchbook.engine.pipeline;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/** The pipeline is plain Java: no networking, no frameworks. Transports live in engine-server. */
@AnalyzeClasses(packages = "dev.matchbook.engine.pipeline", importOptions = ImportOption.DoNotIncludeTests.class)
class PipelineArchitectureTest {

    @ArchTest
    static final ArchRule noNetworkingOrFrameworks = noClasses()
            .should().dependOnClassesThat().resideInAnyPackage(
                    "java.net..", "io.grpc..", "io.netty..", "org.apache.kafka..", "org.springframework..")
            .because("engine-pipeline must stay transport-free so it can be benchmarked and tested in isolation");
}
