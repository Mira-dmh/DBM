import java.io.FileReader;
import java.sql.*;
import java.util.Scanner;
import com.opencsv.CSVReader;

public class test {
    private static final String DB_URL = "jdbc:sqlite:mental_health.db";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(DB_URL);
             Statement statement = connection.createStatement();
             Scanner scanner = new Scanner(System.in)) {

            // Setup tables
            setupTables(statement);

            // Populate database from CSV
            populateDatabaseFromCSV(statement);

            // Main menu loop
            while (true) {
                System.out.println("\nMental Health Database Menu:");
                System.out.println("1. Display all Locations");
                System.out.println("2. Display data for a specific Location");
                System.out.println("3. Display all Data Value Types");
                System.out.println("4. Display data by Data Value Type");
                System.out.println("5. Delete data by Location");
                System.out.println("6. Add new data entry");
                System.out.println("7. Update data for a Location");
                System.out.println("8. Exit");
                System.out.print("Enter your choice: ");
                int choice = scanner.nextInt();
                scanner.nextLine(); // Consume newline

                switch (choice) {
                    case 1 -> displayAllLocations(statement);
                    case 2 -> displayLocationData(statement, scanner);
                    case 3 -> displayAllDataValueTypes(statement);
                    case 4 -> displayDataByValueType(statement, scanner);
                    case 5 -> deleteLocationData(statement, scanner);
                    case 6 -> addDataEntry(statement, scanner);
                    case 7 -> updateLocationData(statement, scanner);
                    case 8 -> {
                        System.out.println("Exiting");
                        return;
                    }
                    default -> System.out.println("Invalid choice. Please try again.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupTables(Statement statement) throws SQLException {
        // Drop tables if they exist
        
        statement.executeUpdate("DROP TABLE IF EXISTS MentalHealth");
        statement.executeUpdate("DROP TABLE IF EXISTS Data");

        // Create MentalHealth table
        statement.executeUpdate("CREATE TABLE MentalHealth ("
                + "RowId TEXT PRIMARY KEY, "
                + "YearStart INTEGER, "
                + "YearEnd INTEGER, "
                + "LocationAbbr VARCHAR, "
                + "LocationDesc VARCHAR, "
                + "Datasource VARCHAR, "
                + "Class VARCHAR, "
                + "Topic VARCHAR, "
                + "Question VARCHAR)"
        );

        // Create Data table
        statement.executeUpdate("CREATE TABLE Data ("
                + "RowId TEXT PRIMARY KEY, "
                + "Data_Value_Unit VARCHAR, "
                + "DataValueTypeID VARCHAR, "
                + "Data_Value_Type VARCHAR, "
                + "Data_Value DOUBLE, "
                + "Geolocation VARCHAR)"
        );
    }

    private static void populateDatabaseFromCSV(Statement statement) {
        try (FileReader filereader = new FileReader("/Users/dingdingjiang/Desktop/Data.csv");
             CSVReader csvReader = new CSVReader(filereader)) {

            String[] nextRecord;
            csvReader.readNext(); // Skip header row

            while ((nextRecord = csvReader.readNext()) != null) {
                // Parse data from CSV
                String rowId = nextRecord[0];
                int yearStart = Integer.parseInt(nextRecord[1]);
                int yearEnd = Integer.parseInt(nextRecord[2]);
                String locationAbbr = nextRecord[3];
                String locationDesc = nextRecord[4];
                String datasource = nextRecord[5];
                String dataClass = nextRecord[6];
                String topic = nextRecord[7];
                String question = nextRecord[8];
                String dataValueUnit = nextRecord[9];
                String dataValueTypeId = nextRecord[10];
                String dataValueType = nextRecord[11];
                String dataValue = nextRecord[12];
                String geolocation = nextRecord[13];

                // Insert into MentalHealth table
                statement.executeUpdate("INSERT INTO MentalHealth (RowId, YearStart, YearEnd, LocationAbbr, LocationDesc, Datasource, Class, Topic, Question) "
                        + "VALUES ('" + rowId + "', " + yearStart + ", " + yearEnd + ", '" + locationAbbr + "', '" + locationDesc + "', '" + datasource + "', '" + dataClass + "', '" + topic + "', '" + question + "')");

                // Insert into Data table
                statement.executeUpdate("INSERT INTO Data (RowId, Data_Value_Unit, DataValueTypeID, Data_Value_Type, Data_Value, Geolocation) "
                        + "VALUES ('" + rowId + "', '" + dataValueUnit + "', '" + dataValueTypeId + "', '" + dataValueType + "', '" + dataValue + "', '" + geolocation + "')");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void displayAllLocations(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT DISTINCT LocationDesc FROM MentalHealth");
        while (rs.next()) {
            System.out.println(rs.getString("LocationDesc"));
        }
    }

    private static void displayLocationData(Statement statement, Scanner scanner) throws SQLException {
        System.out.print("Enter Location Description: ");
        String location = scanner.nextLine();
        
        String query = "SELECT mh.RowId, mh.YearStart, mh.YearEnd, mh.Question, d.Data_Value FROM MentalHealth mh JOIN Data d ON mh.RowId = d.RowId WHERE mh.LocationDesc = ?";
        
        try (PreparedStatement preparedStatement = statement.getConnection().prepareStatement(query)) {
            preparedStatement.setString(1, location);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                System.out.println("RowId: " + rs.getString("RowId"));
                System.out.println("Year: " + rs.getInt("YearStart") + "-" + rs.getInt("YearEnd"));
                System.out.println("Question: " + rs.getString("Question"));
                System.out.println("Data Value: " + rs.getDouble("Data_Value"));
                System.out.println();
            }
        }
    }

    private static void displayAllDataValueTypes(Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT DISTINCT Data_Value_Type FROM Data");
        while (rs.next()) {
            System.out.println(rs.getString("Data_Value_Type"));
        }
    }

    

    private static void deleteLocationData(Statement statement, Scanner scanner) throws SQLException {
        System.out.print("Enter Location Description to delete: ");
        String location = scanner.nextLine();
        statement.executeUpdate("DELETE FROM MentalHealth WHERE LocationDesc = '" + location + "'");
        System.out.println("Data deleted for location: " + location);
    }
    private static void displayDataByValueType(Statement statement, Scanner scanner) throws SQLException {
        System.out.print("Enter Data Value Type: ");
        String valueType = scanner.nextLine();

        // Prepare a single query that joins MentalHealth and Data tables
        String query = "SELECT mh.LocationDesc, d.Data_Value FROM MentalHealth mh JOIN Data d ON mh.RowId = d.RowId WHERE d.Data_Value_Type = ?";

        try (PreparedStatement preparedStatement = statement.getConnection().prepareStatement(query)) {
            preparedStatement.setString(1, valueType);
            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                // Get LocationDesc from MentalHealth table and Data_Value from Data table
                System.out.println("Location: " + rs.getString("LocationDesc") + " - Data Value: " + rs.getDouble("Data_Value"));
            }
        }
    }

    private static void addDataEntry(Statement statement, Scanner scanner) throws SQLException {
        System.out.println("Adding new data entry...");
        System.out.print("Enter RowId: ");
        String rowId = scanner.nextLine();
        System.out.print("Enter Year Start: ");
        int yearStart = scanner.nextInt();
        System.out.print("Enter Year End: ");
        int yearEnd = scanner.nextInt();
        scanner.nextLine(); // Consume newline
        System.out.print("Enter Location Abbreviation: ");
        String locationAbbr = scanner.nextLine();
        System.out.print("Enter Location Description: ");
        String locationDesc = scanner.nextLine();
        System.out.print("Enter Datasource: ");
        String datasource = scanner.nextLine();
        System.out.print("Enter Class: ");
        String dataClass = scanner.nextLine();
        System.out.print("Enter Topic: ");
        String topic = scanner.nextLine();
        System.out.print("Enter Question: ");
        String question = scanner.nextLine();
        System.out.print("Enter Data Value Unit: ");
        String dataValueUnit = scanner.nextLine();
        System.out.print("Enter Data Value TypeID: ");
        String dataValueTypeID = scanner.nextLine();
        System.out.print("Enter Data Value Type: ");
        String dataValueType = scanner.nextLine();
        System.out.print("Enter Data Value: ");
        String dataValue = scanner.nextLine();
        System.out.print("Enter Geolocation: ");
        String geolocation = scanner.nextLine();

        statement.executeUpdate("INSERT INTO MentalHealth (RowId, YearStart, YearEnd, LocationAbbr, LocationDesc, Datasource, Class, Topic, Question) "
                + "VALUES ('" + rowId + "', " + yearStart + ", " + yearEnd + ", '" + locationAbbr + "', '" + locationDesc + "', '" + datasource + "', '" + dataClass + "', '" + topic + "', '" + question + "')");
        statement.executeUpdate("INSERT INTO Data (RowId, Data_Value_Unit, DataValueTypeID, Data_Value_Type, Data_Value, Geolocation)" 
                + "VALUES ('" + rowId + "','"+ dataValueUnit + "', '" + dataValueTypeID + "','" + dataValueType + "', '" + dataValue + "', '" + geolocation + "')");
    }

    private static void updateLocationData(Statement statement, Scanner scanner) throws SQLException {
        System.out.print("Enter RowId to update: ");
        String rowId = scanner.nextLine();

        // Assume there is a field in MentalHealth table that needs to be updated
        System.out.print("Enter new YearStart for MentalHealth: ");
        int newYearStart = scanner.nextInt();
        scanner.nextLine(); // Consume newline
        System.out.print("Enter new Data Value for Data: ");
        double newDataValue = scanner.nextDouble();
        scanner.nextLine(); // Consume newline

        // Update MentalHealth table
        String updateMentalHealthQuery = "UPDATE MentalHealth SET YearStart = ? WHERE RowId = ?";
        try (PreparedStatement preparedStatementMH = statement.getConnection().prepareStatement(updateMentalHealthQuery)) {
            preparedStatementMH.setInt(1, newYearStart);
            preparedStatementMH.setString(2, rowId);
            preparedStatementMH.executeUpdate();
        }

        // Update Data table
        String updateDataQuery = "UPDATE Data SET Data_Value = ? WHERE RowId = ?";
        try (PreparedStatement preparedStatementD = statement.getConnection().prepareStatement(updateDataQuery)) {
            preparedStatementD.setDouble(1, newDataValue);
            preparedStatementD.setString(2, rowId);
            int rowsUpdatedD = preparedStatementD.executeUpdate();
            if (rowsUpdatedD > 0) {
                System.out.println("Data updated successfully for RowId: " + rowId);
            } else {
                System.out.println("No matching RowId found in Data table.");
            }
        }
    }
}

