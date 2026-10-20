package dev.matchbook.engine.core;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/** Keeps the engine core free of frameworks and of anything that would make it non-deterministic. */
@AnalyzeClasses(packages = "dev.matchbook.engine.core", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule onlyJdkAndAgrona = classes()
            .should().onlyDependOnClassesThat()
            .resideInAnyPackage("dev.matchbook.engine.core..", "java..", "org.agrona..")
            .because("engine-core must not depend on Spring or any other framework");

    @ArchTest
    static final ArchRule noClocksOrRandomness = noClasses()
            .should().dependOnClassesThat()
            .haveFullyQualifiedName("java.lang.System")
            .orShould().dependOnClassesThat().resideInAnyPackage("java.time..", "java.util.concurrent..")
            .orShould().dependOnClassesThat().haveFullyQualifiedName("java.util.Random")
            .because("the engine must be deterministic: time comes in on the command, no wall clock");

    @ArchTest
    static final ArchRule noFloatingPointOrBigDecimal = noClasses()
            .should().dependOnClassesThat().haveFullyQualifiedName("java.math.BigDecimal")
            .because("prices and quantities are fixed-point longs");

    @ArchTest
    static final ArchRule noIoOrThreads = noClasses()
            .should().dependOnClassesThat().resideInAnyPackage("java.io..", "java.nio.file..", "java.net..")
            .orShould().dependOnClassesThat().haveFullyQualifiedName("java.lang.Thread")
            .because("I/O and threading belong to the pipeline, not the matching logic");
}
