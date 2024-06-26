import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {
    private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres"; // Replace with your PostgreSQL username
    private static final String PASSWORD = "root"; // Replace with your PostgreSQL password
    private static final int BATCH_SIZE = 10;
    private static final int SLEEP_TIME_MS = 5000; // 5 seconds

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String filePath = "key_value_pairs.txt"; // Path to your key-value file
        List<Map<String, String>> dataList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            Map<String, String> keyValueMap = new HashMap<>();
            int count = 0;

            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty())
                    continue; // Skip empty lines

                String[] keyValue = line.split("=");
                if (keyValue.length == 2) {
                    keyValueMap.put(keyValue[0].trim(), keyValue[1].trim());
                }

                if (keyValueMap.size() == 3) { // Assuming all keys (username, password, email) are present
                    dataList.add(new HashMap<>(keyValueMap));
                    keyValueMap.clear();
                    count++;

                    if (count % BATCH_SIZE == 0) {
                        insertBatch(dataList);
                        dataList.clear();
                        Thread.sleep(SLEEP_TIME_MS);
                    }
                }
            }

            // Insert any remaining data
            if (!dataList.isEmpty()) {
                insertBatch(dataList);
            }

            System.out.println("database updated");

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void insertBatch(List<Map<String, String>> dataList) {
        String sql = "INSERT INTO user_info (username, password, email) VALUES (?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
                PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (Map<String, String> data : dataList) {
                pstmt.setString(1, data.get("username"));
                pstmt.setString(2, data.get("password"));
                pstmt.setString(3, data.get("email"));
                pstmt.addBatch();
            }

            pstmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
