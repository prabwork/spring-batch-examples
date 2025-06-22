package com.prabwork.springbatch.writer

import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.transform.PassThroughLineAggregator
import org.springframework.core.io.FileSystemResource

open class PrefixWriter : FlatFileItemWriter<String>() {

    @BeforeStep
    fun initWriter(stepExecution: StepExecution) {
        val prefix = stepExecution.executionContext.getString(PREFIX)
        setResource(FileSystemResource("data-file-$prefix.txt"))
    }

    init {
        setLineAggregator(PassThroughLineAggregator())
        setLineSeparator(System.lineSeparator())
    }

    companion object {
        private val PREFIX = "PREFIX"
    }
}