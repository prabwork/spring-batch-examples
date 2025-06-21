package com.prabwork.springbatch.config

import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean
import org.springframework.batch.support.DatabaseType
import org.springframework.batch.support.transaction.ResourcelessTransactionManager
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType
import org.springframework.transaction.PlatformTransactionManager
import javax.sql.DataSource

@Configuration
class BatchInitializer {
    @Bean
    fun dataSource(): DataSource {
        val embeddedDatabaseBuilder = EmbeddedDatabaseBuilder()
        return embeddedDatabaseBuilder.addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
            .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
            .setType(EmbeddedDatabaseType.H2)
            .build()
    }

    @Bean
    fun transactionManager(): ResourcelessTransactionManager {
        return ResourcelessTransactionManager()
    }

    @Bean
    @Throws(Exception::class)
    @Primary
    fun jobRepositoryFactoryBean(
        dataSource: DataSource,
        transactionManager: PlatformTransactionManager,
    ): JobRepositoryFactoryBean {
        val factory = JobRepositoryFactoryBean()
        factory.setDatabaseType(DatabaseType.H2.productName)
        factory.setDataSource(dataSource)
        factory.transactionManager = transactionManager
        return factory
    }

    @Bean
    fun jobRespository(jobRepositoryFactoryBean: JobRepositoryFactoryBean): JobRepository {
        return jobRepositoryFactoryBean.`object`
    }

}
