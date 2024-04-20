import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.sql.Date;


public class CWRUMDb {

    private static final String CONNECTION_URL = "jdbc:sqlserver://cxp-sql-02\\adr114;"
    + "database=CwruMDb;"
    + "user=cwrumdb;"
    + "password=4321$#@!vorabaz;"
    + "encrypt=true;"
    + "trustServerCertificate=true;"
    + "loginTimeout=15;";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(CONNECTION_URL);
             Scanner scanner = new Scanner(System.in)) {

            boolean exit = false;
            while (!exit) {
                displayMenu();
                int choice = scanner.nextInt();
                scanner.nextLine(); // Consume the newline character

                switch (choice) {
                    case 1:
                        executeGetUserReviewsByGenre(connection, scanner);
                        break;
                    case 2:
                        executeGetMostActiveUsers(connection);
                        break;
                    case 3:
                        executeDeleteUserAccount(connection, scanner);
                        break;
                    case 4:
                        executeTransferUserReview(connection, scanner);
                        break;    
                    case 0:
                        exit = true;
                        break;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }

                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void displayMenu() {
        System.out.println("Menu:");
        System.out.println("1. Get User Reviews by Genre");
        System.out.println("2. Get Most Active Users");
        System.out.println("3. Delete User Account");
        System.out.println("4. Transfer User Review");
        System.out.println("0. Exit");
        System.out.print("Enter your choice: ");
    }

    private static void executeGetUserReviewsByGenre(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter username: ");
        String username = scanner.nextLine();

        System.out.print("Enter genre name: ");
        String genreName = scanner.nextLine();

        String callGetUserReviewsByGenre = "{call dbo.GetUserReviewsByGenre(?, ?)}";
        try (CallableStatement statement = connection.prepareCall(callGetUserReviewsByGenre)) {
            statement.setString(1, username);
            statement.setString(2, genreName);

            try (ResultSet resultSet = statement.executeQuery()) {
                System.out.println("User Reviews by Genre:");
                while (resultSet.next()) {
                    String title = resultSet.getString("title");
                    String reviewText = resultSet.getString("review_text");
                    int rating = resultSet.getInt("rating");
                    java.sql.Date reviewDate = resultSet.getDate("review_date");

                    System.out.println("Title: " + title);
                    System.out.println("Review: " + reviewText);
                    System.out.println("Rating: " + rating);
                    System.out.println("Review Date: " + reviewDate);
                    System.out.println("---");
                }
            }
        }
    }

    private static void executeGetMostActiveUsers(Connection connection) throws SQLException {
        String callGetMostActiveUsers = "{call dbo.GetMostActiveUsers}";
        try (CallableStatement statement = connection.prepareCall(callGetMostActiveUsers);
                ResultSet resultSet = statement.executeQuery()) {
            System.out.println("Most Active Users:");
            while (resultSet.next()) {
                String activeUsername = resultSet.getString("username");
                int reviewCount = resultSet.getInt("review_count");

                System.out.println("Username: " + activeUsername);
                System.out.println("Review Count: " + reviewCount);
                System.out.println("---");
            }
        }
    }

    private static void executeDeleteUserAccount(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter username to delete: ");
        String username = scanner.nextLine();
    
        String callGetUserDetails = "{call dbo.GetUserDetails(?)}";
        try (CallableStatement getUserDetailsStmt = connection.prepareCall(callGetUserDetails)) {
            getUserDetailsStmt.setString(1, username);
            
            try (ResultSet userDetailsResultSet = getUserDetailsStmt.executeQuery()) {
                if (userDetailsResultSet.next()) {
                    String email = userDetailsResultSet.getString("email");
                    Date joinDate = userDetailsResultSet.getDate("join_date");
    
                    System.out.println("User Details:");
                    System.out.println("Username: " + username);
                    System.out.println("Email: " + email);
                    System.out.println("Join Date: " + joinDate);
                    System.out.print("Are you sure you want to delete this account? (yes/no): ");
                    
                    String confirmation = scanner.nextLine().trim().toLowerCase();
                    if ("yes".equals(confirmation)) {
                        String callDeleteUserAccount = "{call dbo.DeleteUserAccount(?, ?)}";
                        try (CallableStatement deleteUserStmt = connection.prepareCall(callDeleteUserAccount)) {
                            deleteUserStmt.setString(1, username);
                            deleteUserStmt.registerOutParameter(2, java.sql.Types.INTEGER);

                            deleteUserStmt.execute();
                            int deletedUserId = deleteUserStmt.getInt(2);
                            
                            if (deletedUserId > 0) System.out.println("User deleted successfully. User ID: " + deletedUserId);
                            else System.out.println("Failed to delete user.");
                        }
                    } else System.out.println("Deletion cancelled by user.");
                    
                } else System.out.println("No user found with the username: " + username);
                
            }
        }
    }

    private static void executeTransferUserReview(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter username: ");
        String username = scanner.nextLine();

        System.out.print("Enter original movie title: ");
        String originalMovieTitle = scanner.nextLine();

        System.out.print("Enter new movie title: ");
        String newMovieTitle = scanner.nextLine();

        String callTransferUserReview = "{call TransferUserReview(?, ?, ?, ?)}";
        try (CallableStatement statement = connection.prepareCall(callTransferUserReview)) {
            statement.setString(1, username);
            statement.setString(2, originalMovieTitle);
            statement.setString(3, newMovieTitle);
            statement.registerOutParameter(4, java.sql.Types.NVARCHAR);

            statement.execute();

            String resultMessage = statement.getString(4);
            System.out.println("Result: " + resultMessage);
        }
    }
    
}

    
