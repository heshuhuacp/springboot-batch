package com.example.batch.config;

import com.example.batch.domain.WorkMan;
import com.example.batch.listener.CsvJobListener;
import com.example.batch.process.CsvItemProcessor;
import com.example.batch.validate.CsvBeanValidator;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.validator.Validator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
/**
 * 使用EnableBatchProcessing开启批处理的支持
 */
@Configuration
@EnableBatchProcessing
public class CsvBatchConfig {

    @Bean
    public ItemReader<WorkMan> reader(){

        //使用FlatFileItemReader读取文件
        FlatFileItemReader<WorkMan> reader = new FlatFileItemReader<WorkMan>();
        //使用setResource方法设置csv文件的路径
        reader.setResource(new ClassPathResource("people.csv"));
        reader.setLineMapper(new DefaultLineMapper<WorkMan>(){{
            //对cvs文件的数据和领域模型做对应映射
            setLineTokenizer(new DelimitedLineTokenizer(){{
                setNames(new String[]{"name", "age", "nation", "address"});
            }});

            setFieldSetMapper(new BeanWrapperFieldSetMapper<WorkMan>(){{
                setTargetType(WorkMan.class);
            }});
        }});
        return reader;
    }

    @Bean
    public ItemProcessor<WorkMan, WorkMan> processor(){
        CsvItemProcessor processor = new CsvItemProcessor();
        processor.setValidator(csvBeanValidator());
        return processor;
    }

    @Bean
    public ItemWriter<WorkMan> writer(DataSource dataSource){
        //使用JdbcBatchItemWriter批处理来写数据到到数据库h
        JdbcBatchItemWriter<WorkMan> writer = new JdbcBatchItemWriter<WorkMan>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        String sql = "insert into workman " + "(name,age,nation,address) "
                +"values(:name, :age, :nation, :address)";
        writer.setSql(sql);
        writer.setDataSource(dataSource);
        return writer;

    }

    @Bean
    public JobRepository jobRepository(DataSource dataSource
            , PlatformTransactionManager transactionManager) throws Exception{

        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setDataSource(dataSource);
        jobRepositoryFactoryBean.setTransactionManager(transactionManager);
        jobRepositoryFactoryBean.setDatabaseType("mysql");
        return jobRepositoryFactoryBean.getObject();
    }

    @Bean
    public SimpleJobLauncher jobLauncher(DataSource dataSource
            , PlatformTransactionManager transactionManager) throws Exception{

        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository(dataSource, transactionManager));
        return jobLauncher;
    }

    @Bean
    public Job importJob(JobBuilderFactory jobs, Step s1){
hh    }

    /**
     * 注入上面声明的reader，writer，processor
     * @param stepBuilderFactory
     * @param reader
     * @param writer
     * @param processor
     * @return
     */
    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<WorkMan> reader
            , ItemWriter<WorkMan> writer, ItemProcessor<WorkMan, WorkMan> processor){

        return stepBuilderFactory
                .get("step1")
                .<WorkMan, WorkMan>chunk(65000)//每次提交65000调数据
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();

    }

    /**
     * job监听
     * @return
     */
    @Bean
    public CsvJobListener csvJobListener(){
        return new CsvJobListener();
    }

    /**
     * 校验器
     * @return
     */
    @Bean
    public Validator<WorkMan> csvBeanValidator(){
        return new CsvBeanValidator<WorkMan>();
    }


}
