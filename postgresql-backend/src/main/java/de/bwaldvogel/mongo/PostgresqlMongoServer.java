package de.bwaldvogel.mongo;

import de.bwaldvogel.mongo.backend.postgresql.PostgresqlBackend;
import org.postgresql.jdbc3.Jdbc3PoolingDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class PostgresqlMongoServer extends MongoServer {

    private static final Logger log = LoggerFactory.getLogger(PostgresqlMongoServer.class);

    private static Jdbc3PoolingDataSource dataSource;

    public static void main(String[] args) throws Exception {
        final MongoServer mongoServer = new PostgresqlMongoServer();
        mongoServer.bind("localhost", 27017);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("shutting down {}", mongoServer);
                mongoServer.shutdownNow();
                closeDataSource();
            }
        });
    }

    public PostgresqlMongoServer() {
        super(new PostgresqlBackend(initializeDataSource("mongo-java-server-test", "mongo-java-server-test", "mongo-java-server-test")));
    }

    public static DataSource initializeDataSource(String databaseName, String user, String password) {
        Jdbc3PoolingDataSource dataSource = new Jdbc3PoolingDataSource();
        dataSource.setDataSourceName(PostgresqlMongoServer.class.getSimpleName());
        dataSource.setApplicationName(PostgresqlMongoServer.class.getSimpleName());
        dataSource.setDatabaseName("mongo-java-server-test");
        dataSource.setUser("mongo-java-server-test");
        dataSource.setPassword("mongo-java-server-test");
        dataSource.setMaxConnections(5);
        return dataSource;
    }

    public static void closeDataSource() {
        dataSource.close();
    }

}
