package com.example.sitemoitor;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import com.alibaba.druid.support.ibatis.DruidDataSourceFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

@SpringBootApplication
public class SiteMonitorApplication {

    private static final Logger logger = LoggerFactory.getLogger(SiteMonitorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SiteMonitorApplication.class, args);
    }

//    @Bean
//    public JdbcTemplate jdbcTemplate(DataSourceBuilder dataSourceBuilder) {
//        DataSource dataSource = dataSourceBuilder.build();
//        return new JdbcTemplate(dataSource);
//    }

    @Bean
    public DataSource dataSource() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean
    @Autowired
    public CommandLineRunner commandLineRunner(JdbcTemplate jdbcTemplate) {

        return args -> {

            String url = "https://admission.pku.edu.cn";
            Document document = Jsoup.connect(url).get();

            final Elements a = document.select("a");

//            a.forEach(t -> {
//                logger.info("{},{},{}", t.text(), t.attr("title"), t.attr("href"));
//            });

            String sql = "INSERT INTO links (doman, href, info, title, version, created_at, sum) VALUE (?, ?, ?, ?, ?, now(), ?) ON DUPLICATE KEY UPDATE touch_at = now();";

            int[] ints = jdbcTemplate.batchUpdate(sql, new LinkBatchPreparedStatementSetter(url, url, Instant.now().toString(), a));

            List<String> newMessage = Arrays.stream(ints)
                    .boxed()
                    .filter(t -> 1 == t)
                    .map(a::get)
                    .map(t -> {
                        return String.format("%s,%s,%s", t.text(), t.attr("title"), t.attr("href"));
                    }).collect(Collectors.toList());

            logger.info("update {}", Arrays.stream(ints).boxed().collect(Collectors.toList()));

            logger.info("get new message {}", newMessage);


        };
    }

    public static class LinkBatchPreparedStatementSetter implements BatchPreparedStatementSetter {
        Elements elements;

        String domain;
        String baseHref;
        String version;

        public LinkBatchPreparedStatementSetter(String domain, String baseHref, String version, Elements elements) {
            this.domain = domain;
            this.baseHref = baseHref;
            this.version = version;
            this.elements = elements;
        }

        @Override
        public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {

            Element element = elements.get(i);

            preparedStatement.setString(1, domain);

            String href = element.attr("href");
            href = StringUtils.isEmpty(href) ? "NONE" : href;

            preparedStatement.setString(2, href);

            String text = element.text();
            text = StringUtils.isEmpty(text) ? "NONE" : text;

            preparedStatement.setString(3, text);

            String title = element.attr("title");

            title = StringUtils.isEmpty(title) ? "NONE" : title;

            preparedStatement.setString(4, title);

            preparedStatement.setString(5, version);

            String sum = DigestUtils.md5DigestAsHex((domain + href + title + text).getBytes());

            preparedStatement.setString(6, sum);

        }

        @Override
        public int getBatchSize() {
            return elements.size();
        }
    }

}
