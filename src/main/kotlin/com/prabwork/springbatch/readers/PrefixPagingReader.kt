package com.prabwork.springbatch.readers

import org.slf4j.LoggerFactory
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.item.database.AbstractPagingItemReader
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.LinkedList

open class PrefixPagingReader : AbstractPagingItemReader<String>() {
    private var context: ExecutionContext? = null

    private var reader: BufferedReader? = null

    @BeforeStep
    fun initStep(stepExecution: StepExecution) {
        val context = stepExecution.executionContext
        val prefix = context.getString(PREFIX)

        this.context = context
        reader = this.javaClass.classLoader.getResourceAsStream("datafile.txt")
            .let {
                BufferedReader(InputStreamReader(it))
            }
        LOGGER.info("For Prefix $prefix In before step method and This is $this and hashcode is ${this.hashCode()}")
    }

    @AfterStep
    fun postJob() {
        reader?.close()
    }

    override fun doReadPage() {
        var pageSize = pageSize
        val prefix = context?.getString(PREFIX) ?: ""

        if (results == null) {
            results = LinkedList()
        } else {
            results.clear()
        }
        do {
            val line = reader?.readLine()
            val match = (line ?: "").let {
                it.startsWith(prefix.lowercase()) || it.startsWith(prefix.uppercase())
            }
            if (match) {
                results.add(line)
                pageSize = pageSize - 1
            }
        } while (pageSize > 0 && line != null)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PrefixPagingReader::class.java)
        private const val PREFIX = "PREFIX"
    }
}