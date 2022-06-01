package org.example;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class CacheIssueTest {

    public static DockerImageName imageName(String registry, String image, String version) {
        return DockerImageName
                .parse(registry + "/" + image + ":" + version)
                .asCompatibleSubstituteFor(image);
    }

    /**
     * The test case: update and delete rows from a table using a temporary table
     */
    protected void test(MySQLContainer<?> mysql, boolean cacheEnabled) throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", mysql.getUsername());
        connectionProps.put("password", mysql.getPassword());
        connectionProps.put("useServerPrepStmts", "true");
        connectionProps.put("prepStmtCacheSqlLimit", "2048");
        connectionProps.put("cachePrepStmts", String.valueOf(cacheEnabled));
        //connectionProps.put("sslMode", "DISABLED"); // to allow traffic inspection with Wireshark

        try (Connection conn = DriverManager.getConnection(mysql.getJdbcUrl(), connectionProps)) {
            // Setup
            try (PreparedStatement ps = conn.prepareStatement("DROP TABLE IF EXISTS Book")) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE Book (id INTEGER NOT NULL, title VARCHAR(255), PRIMARY KEY (id)) ENGINE = InnoDB")) {
                ps.executeUpdate();
            }

            // We only have one element in table Book
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Book (id, title) VALUES (1, 'MySQL and JSON')")) {
                ps.executeUpdate();
            }

            // Update
            try (PreparedStatement ps = conn.prepareStatement("CREATE TEMPORARY TABLE IF NOT EXISTS ht_Book (id INTEGER NOT NULL)")) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ht_Book SELECT sub.id FROM (SELECT id, title FROM Book UNION ALL SELECT id, title FROM Book) sub WHERE sub.title = ?")) {
                ps.setString(1, "MySQL and JSON");
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("UPDATE Book SET title=CONCAT(title, ?) WHERE (id) IN (SELECT id FROM ht_Book)")) {
                ps.setString(1, ": A Practical Programming Guide");
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("DROP TEMPORARY TABLE ht_Book")) {
                ps.executeUpdate();
            }

            // Read after updating the title
            try (
                    PreparedStatement ps = conn.prepareStatement("SELECT * FROM Book");
                    ResultSet rs = ps.executeQuery()
            ) {
                assertTrue(rs.next(), "We expected one result after the update");
                assertEquals(1, rs.getInt("id"));
                assertEquals("MySQL and JSON: A Practical Programming Guide", rs.getString("title"));
                assertFalse(rs.next(), "Unexpected result returned after the update");
            }

            // Delete
            try (PreparedStatement ps = conn.prepareStatement("CREATE TEMPORARY TABLE IF NOT EXISTS ht_Book (id INTEGER NOT NULL)")) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ht_Book SELECT sub.id FROM (SELECT id, title FROM Book UNION ALL SELECT id, title FROM Book) sub WHERE sub.title = ?")) {
                ps.setString(1, "MySQL and JSON: A Practical Programming Guide");
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM Book WHERE (id) IN (SELECT id FROM ht_Book)")) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("DROP TEMPORARY TABLE ht_Book")) {
                ps.executeUpdate();
            }

            // Should have deleted all the rows
            try (
                    PreparedStatement ps = conn.prepareStatement("SELECT * FROM Book");
                    ResultSet rs = ps.executeQuery()
            ) {
                assertFalse(rs.next(), "We didn't expect any result after the delete");
            }
        }
    }

    /**
     * Test using MySQL 8.0.29
      */
    public static class v8029Test extends CacheIssueTest {
        public static final MySQLContainer<?> MYSQL = new MySQLContainer<>(imageName("docker.io", "mysql", "8.0.29"))
                .withReuse(true);

        @BeforeAll
        public static void before() {
            MYSQL.start();
        }

        @Test
        public void testWithCacheEnabled() throws Exception {
            test(MYSQL, true);
        }

        @Test
        public void testWithCacheDisabled() throws Exception {
            test(MYSQL, false);
        }
    }

    /**
     * Test using MySQL 8.0.29
     */
    public static class v8028Test extends CacheIssueTest {
        public static final MySQLContainer<?> MYSQL = new MySQLContainer<>(imageName("docker.io", "mysql", "8.0.28"))
                .withReuse(true);

        @BeforeAll
        public static void before() {
            MYSQL.start();
        }

        @Test
        public void testWithCacheEnabled() throws Exception {
            test(MYSQL, true);
        }

        @Test
        public void testWithCacheDisabled() throws Exception {
            test(MYSQL, false);
        }
    }

}
