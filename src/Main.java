import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        Connection connection = ConnectDB.connect();

        String getAlbums = "src/AlbumsScraper.py";
        String getSongs = "src/SongsScraper.py";

        runPythonScript(getAlbums);
        runPythonScript(getSongs);

        String artistAlbum = "output/album_list.txt";

//        importTracksAndAlbums(connection, artistAlbum);
        dropAlbumTables(connection);

    }

    private static void runPythonScript(String fileName) throws IOException {
        ProcessBuilder pb = new ProcessBuilder("venv/scripts/python", resolvePath(fileName));
        pb.redirectErrorStream(true);
        Process process = pb.start();

        List<String> output = readProcessOutput(process.getInputStream());
        for (String s : output) {
            System.out.println(s);
        }
    }

    private static String resolvePath(String fileName) {
        File file = new File(fileName);
        return file.getAbsolutePath();
    }

    private static List<String> readProcessOutput(InputStream inputStream) throws IOException {
        try (BufferedReader output = new BufferedReader(new InputStreamReader(inputStream))) {
            return output.lines().collect(Collectors.toList());
        }
    }

    private static void importTracksAndAlbums(Connection connection, String source) throws IOException {

        try {
            File albumsSource = new File(resolvePath(source));
            BufferedReader br = new BufferedReader(new FileReader(albumsSource));
            String albumsLine = br.readLine();

            while (albumsLine != null) {
                createTable(connection, albumsLine);
                File tracksSource = new File(resolvePath("output/" + albumsLine + ".csv"));
                BufferedReader tr = new BufferedReader(new FileReader(tracksSource));
                tr.readLine();      // Skip first line (Column names)
                String tracksLine = tr.readLine();

                while (tracksLine != null) {
                    String[] values = tracksLine.split("\\|");
                    addRow(connection, albumsLine, values);
                    tracksLine = tr.readLine();
                }

                albumsLine = br.readLine();
            }

            System.out.println("Successfully imported all files to the database.");

        } catch (IOException | SQLException e) {
            System.out.println("An error occurred: ");
            e.printStackTrace();
        }

    }

    private static void createAlbumsTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE ALBUMS\n" +
                     "(\n" +
                     "ALBUM_ID      NUMBER CONSTRAINT pk_album_id PRIMARY KEY,\n" +
                     "TITLE         VARCHAR2(125),\n" +
                     "ARTIST        VARCHAR2(125),\n" +
                     "YEAR_RELEASED NUMBER,\n" +
                     "URL           VARCHAR2(125)\n" +
                     ")";
        try {
            Statement statement = connection.createStatement();
            statement.execute(sql);
            System.out.println("Table successfully created");
        } catch (SQLException e) {
            System.out.println("An error occurred: ");
            e.printStackTrace();
        }
    }

    private static void createTable(Connection connection, String tableName) throws SQLException {
        tableName = formatString(tableName);
        System.out.println(tableName);
        String sql = "CREATE TABLE \"" + tableName + "\" \n" +
                     "(\n" +
                     "TRACK_ID      NUMBER,\n" +
                     "ALBUM_ID      NUMBER,\n" +
                     "TRACK_NO      NUMBER,\n" +
                     "TITLE         VARCHAR2(150),\n" +
                     "DURATION      NUMBER,\n" +
                     "CONSTRAINT \"pk_track_id_" + tableName + "\" PRIMARY KEY (TRACK_ID)\n" +
//                     "CONSTRAINT fk_album_id_" + tableName + " FOREIGN KEY (ALBUM_ID) REFERENCES ALBUMS(ALBUM_ID)\n" +
                     ")";
        try {
            Statement statement = connection.createStatement();
            statement.execute(sql);
            statement.close();
        } catch (SQLException e) {
            System.out.println("An error occurred: ");
            e.printStackTrace();
        }
    }

    private static void addRow(Connection connection, String tableName, String[] values) throws SQLException {
        String sql = "INSERT INTO \"" + formatString(tableName) + "\" VALUES(?, ?, ?, ?, ?)";
        PreparedStatement cs = connection.prepareStatement(sql);
        for (int i=0; i<values.length; i++) {
            cs.setString(i+1, values[i]);
        }
        cs.executeUpdate();
        cs.close();
    }


    // FOR DEBUGGING PURPOSES ONLY
    private static void dropAlbumTables(Connection connection) throws IOException {
        try {
            String source = "output/album_list.txt";
            File tablesSource = new File(resolvePath(source));
            BufferedReader br = new BufferedReader(new FileReader(tablesSource));
            String table = br.readLine();

            while (table != null) {
                dropTable(connection, table);
                table = br.readLine();
            }
            System.out.println("Successfully dropped tables");

        } catch (IOException | SQLException e) {
            System.out.println("An error occurred: ");
            e.printStackTrace();
        }
    }

    private static void dropTable(Connection connection, String tableName) throws SQLException{
        try {
            String sql = "DROP TABLE \"" + formatString(tableName) + "\"";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            System.out.println("An error occurred: ");
            e.printStackTrace();
        }
    }

    private static String formatString(String text) {
        text = text.toUpperCase();
        text = text.replace("–", "-");
        return text.replace("&", "_AND_");
    }

}