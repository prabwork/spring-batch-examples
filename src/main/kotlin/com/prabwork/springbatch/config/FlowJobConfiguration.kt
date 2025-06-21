package com.prabwork.springbatch.config

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameter
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.transaction.PlatformTransactionManager

@Configuration
class FlowJobConfiguration {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(FlowJobConfiguration::class.java)
    }

    @Bean
    fun flowJob(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager
    ): Job {
        val flowDecider = flowDecider()
        return JobBuilder("flow-job-ex", jobRepository)
            .start(flowDecider)
            .from(flowDecider).on("ALPHA")
            .to(alphaStep(jobRepository, transactionManager))
            .from(flowDecider).on("DIGIT")
            .to(digitStep(jobRepository, transactionManager))
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
            .tasklet({ contribution, _ ->
                val context = contribution.stepExecution.executionContext
                val alpha = context.getString("ALPHA")
                LOGGER.info("ALPHA for this step is $alpha")
                RepeatStatus.FINISHED
            }, transactionManager)
            .build()
    }

    fun digitWorkerStep(
        jobRepository: JobRepository,
        transactionManager: PlatformTransactionManager
    ): Step {
        return StepBuilder("step-digit-worker", jobRepository)
            .tasklet({ contribution, _ ->
                val context = contribution.stepExecution.executionContext
                val digit = context.getInt("DIGIT")
                LOGGER.info("DIGIT for this step is $digit")
                RepeatStatus.FINISHED
            }, transactionManager)
            .build()
    }

    fun alphaPartitioner(): Partitioner {
        return Partitioner { _ ->
            val alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            val contexts = mutableMapOf<String, ExecutionContext>()
            for (alpha in alphabets) {
                contexts["ALPHA-$alpha"] = ExecutionContext().apply {
                    putString("ALPHA", alpha.toString())
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
                    putInt("DIGIT", digit)
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
        val parameter: JobParameter<String> = JobParameter("DIGIT", String::class.java)
        val params = mutableMapOf<String, JobParameter<*>>(Pair("route", parameter))
        return CommandLineRunner {
            jobLauncher.run(flowJob, JobParameters(params))
        }
    }
}
