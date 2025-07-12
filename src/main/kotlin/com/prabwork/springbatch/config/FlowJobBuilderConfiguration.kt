package com.prabwork.springbatch.config

import com.prabwork.springbatch.readers.PrefixPagingReader
import com.prabwork.springbatch.writer.PrefixWriter
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameter
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.builder.FlowJobBuilder
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ExecutionContext
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class FlowJobBuilderConfiguration {
    companion object {
        private const val PREFIX = "PREFIX"
    }

    @Bean
    fun flowJob(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager
    ): Job {
        val flowDecider = flowDecider()
        val alphaFlow: Flow = FlowBuilder<Flow>("alpha-step")
            .start(alphaStep(jobRepository, transactionManager))
            .end()
        val digitFlow: Flow = FlowBuilder<Flow>("digit-step")
            .start(digitStep(jobRepository, transactionManager))
            .end()
        return FlowJobBuilder(JobBuilder("flow-job-builder-ex", jobRepository))
            .start(flowDecider)
            .from(flowDecider)
            .on("DIGIT")
            .to(digitFlow)
            .from(flowDecider)
            .on("ALPHA")
            .to(alphaFlow)
            .end()
            .build()
    }

    fun digitStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager
    ): Step {
        return StepBuilder(
            "digit-step", jobRepository
        ).partitioner("step-digit-worker", digitPartitioner())
            .step(digitWorkerStep(jobRepository, transactionManager))
            .gridSize(1)
            .build()
    }

    fun alphaStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager
    ): Step {
        return StepBuilder("alpha-step", jobRepository)
            .partitioner("step-alpha-worker", alphaPartitioner())
            .step(alphaWorkerStep(jobRepository, transactionManager))
            .gridSize(1)
            .build()
    }

    fun alphaWorkerStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager
    ): Step {
        return StepBuilder("step-alpha-worker", jobRepository)
            .chunk<String, String>(10, transactionManager)
            .reader(prefixPagingReader())
            .writer(prefixWriter())
            .build()
    }

    fun digitWorkerStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager
    ): Step {
        return StepBuilder("step-digit-worker", jobRepository)
            .chunk<String, String>(10, transactionManager)
            .reader(prefixPagingReader())
            .writer(prefixWriter())
            .build()
    }

    @Bean
    @StepScope
    fun prefixPagingReader(): PrefixPagingReader = PrefixPagingReader()

    @Bean
    @StepScope
    fun prefixWriter(): PrefixWriter = PrefixWriter()

    fun alphaPartitioner(): Partitioner {
        return Partitioner { _ ->
            val alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            val contexts = mutableMapOf<String, ExecutionContext>()
            for (alpha in alphabets) {
                contexts["ALPHA-$alpha"] = ExecutionContext().apply {
                    putString(PREFIX, alpha.toString())
                }
            }
            contexts
        }
    }

    fun digitPartitioner(): Partitioner {
        return Partitioner { _ ->
            val contexts = mutableMapOf<String, ExecutionContext>()
            for (digit in 0..9) {
                contexts["DIGIT-$digit"] = ExecutionContext().apply {
                    putString(PREFIX, digit.toString())
                }
            }
            contexts
        }
    }

    fun flowDecider(): JobExecutionDecider {
        return JobExecutionDecider { jobExecution, _ ->
            val route: String = jobExecution.jobParameters.getString("route") ?: "ALPHA"
            FlowExecutionStatus(route)
        }
    }

    @Bean
    @DependsOn("flowJob")
    fun commandLineListener(
        jobLauncher: JobLauncher,
        flowJob: Job,
    ): CommandLineRunner {
        val alphaParam: JobParameter<String> = JobParameter("ALPHA", String::class.java)
        val alphaParams = mutableMapOf<String, JobParameter<*>>(Pair("route", alphaParam))
        val digitParam: JobParameter<String> = JobParameter("DIGIT", String::class.java)
        val digitParams = mutableMapOf<String, JobParameter<*>>(Pair("route", digitParam))
        return CommandLineRunner {
            jobLauncher.run(flowJob, JobParameters(alphaParams))
            jobLauncher.run(flowJob, JobParameters(digitParams))
        }
    }
}
