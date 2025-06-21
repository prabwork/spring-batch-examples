package com.prabwork.springbatch

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableBatchProcessing
class SpringBatchExamplesApplication

fun main(args: Array<String>) {
    runApplication<SpringBatchExamplesApplication>(*args)
}
