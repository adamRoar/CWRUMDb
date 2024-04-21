import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
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
                    case 5:
                        executeListMoviesByDirector(connection, scanner);  
                        break;
                    case 6:
                        executeInsertReview(connection, scanner);  
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
        System.out.println("5. List Movies by Director");
        System.out.println("6. Insert Review");
        System.out.println("0. Exit");
        System.out.print("Enter your choice: ");
    }

    private static void executeListMoviesByDirector(Connection connection, Scanner scanner) {
        System.out.print("Enter director first name: ");
        String firstName = scanner.nextLine();

        System.out.print("Enter director last name: ");
        String lastName = scanner.nextLine();

        String callListMoviesByDirector = "{call dbo.ListMoviesByDirector(?, ?)}";
        try (CallableStatement statement = connection.prepareCall(callListMoviesByDirector)) {
            statement.setString(1, firstName);
            statement.setString(2, lastName);

            try (ResultSet resultSet = statement.executeQuery()) {
                System.out.println("Movies by " + firstName + " " + lastName + ":");
                while (resultSet.next()) {
                    int movieId = resultSet.getInt("movie_id");
                    String title = resultSet.getString("title");
                    int runtime = resultSet.getInt("runtime");
                    String plotSummary = resultSet.getString("plot_summary");

                    System.out.println("ID: " + movieId);
                    System.out.println("Title: " + title);
                    System.out.println("Runtime: " + runtime);
                    System.out.println("Plot summary : " + plotSummary);
                    System.out.println("---");
                }
            }
        }
        catch (SQLException e) {
            System.err.println("Error fetching list of movies by director: ");
            System.out.println(e);
        }
    }

    public static void executeInsertReview(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter username: ");
        String username = scanner.nextLine();

        System.out.print("Enter password: ");
        String password = scanner.nextLine();

        if (!authenticateUser(connection, username, password)) {
            System.out.println("Authentication failed. Access denied.");
            return; 
        }

        System.out.print("Enter review text: ");
        String reviewText = scanner.nextLine();

        boolean validRating = false;
        int rating = 0;
        while (!validRating) {
            System.out.print("Enter rating (1, 2, 3, 4, or 5): ");
            String ratingStr = scanner.nextLine();
            try {
                int ratingInt = Integer.parseInt(ratingStr);
                if (ratingInt > 5 || ratingInt < 1) {
                    throw new NumberFormatException();
                }
                rating = ratingInt;
                validRating = true;

            } catch (NumberFormatException e) {
                System.out.println("Invalid rating entered, please enter an integer between 1 and 5.");
            }
        }
        
        System.out.print("Enter movie title: ");
        String movieTitle = scanner.nextLine();

        String callInsertUserReview = "{call InsertUserReview(?, ?, ?, ?)}";

        try (CallableStatement insertStatement = connection.prepareCall(callInsertUserReview)) {
            insertStatement.setString(1, username);
            insertStatement.setString(2, reviewText);
            insertStatement.setInt(3, rating);
            insertStatement.setString(4, movieTitle);

            try {
                insertStatement.execute();
                System.out.println("Review inserted: " + rating + "/5 for " + movieTitle);
            } catch (SQLException e) {
                System.err.println("Movie not found, please try again.");
            }
        }
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

        System.out.print("Enter password: ");
        String password = scanner.nextLine();

        if (!authenticateUser(connection, username, password)) {
            System.out.println("Authentication failed. Access denied.");
            return; 
        }
    
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
                            
                            if (deletedUserId > 0) {
                                System.out.println("User deleted successfully. User ID: " + deletedUserId);
                                verifyUserDeletion(connection, username);
                            } 
                            else System.out.println("Failed to delete user.");
                        }
                    } else System.out.println("Deletion cancelled by user.");
                    
                } else System.out.println("No user found with the username: " + username);
                
            }
        }
    }

    public static void executeTransferUserReview(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter username: ");
        String username = scanner.nextLine();

        System.out.print("Enter password: ");
        String password = scanner.nextLine();

        if (!authenticateUser(connection, username, password)) {
            System.out.println("Authentication failed. Access denied.");
            return; 
        }

        System.out.print("Enter original movie title: ");
        String originalMovieTitle = scanner.nextLine();

        System.out.print("Enter new movie title: ");
        String newMovieTitle = scanner.nextLine();

        String callTransferUserReview = "{call TransferUserReview(?, ?, ?, ?)}";

        try (CallableStatement transferStatement = connection.prepareCall(callTransferUserReview)) {
            transferStatement.setString(1, username);
            transferStatement.setString(2, originalMovieTitle);
            transferStatement.setString(3, newMovieTitle);
            transferStatement.registerOutParameter(4, java.sql.Types.NVARCHAR);

            transferStatement.execute();

            String transferResult = transferStatement.getString(4);
            System.out.println("Transfer Result: " + transferResult);

            if (transferResult.equals("Review transferred successfully.")) {
                verifyReviewTransfer(connection, username, originalMovieTitle, newMovieTitle);
            }

        }
    }
    
    private static void verifyReviewTransfer(Connection connection, String username, String originalMovieTitle, String newMovieTitle) throws SQLException {
        String callVerifyReviewTransfer = "{call VerifyUserReviewTransfer(?, ?, ?, ?)}";

        try (CallableStatement verifyStatement = connection.prepareCall(callVerifyReviewTransfer)) {
            verifyStatement.setString(1, username);
            verifyStatement.setString(2, originalMovieTitle);
            verifyStatement.setString(3, newMovieTitle);

            verifyStatement.registerOutParameter(4, java.sql.Types.NVARCHAR);

            verifyStatement.execute();

            String verifyResult = verifyStatement.getString(4);
            System.out.println("Verification Result: " + verifyResult);
        }
    }

    private static void verifyUserDeletion(Connection connection, String username) throws SQLException {
        String storedProcedureCall = "{call VerifyUserDeletion(?, ?)}";

        try (CallableStatement callableStatement = connection.prepareCall(storedProcedureCall)) {
            callableStatement.setString(1, username);
            callableStatement.registerOutParameter(2, java.sql.Types.NVARCHAR);

            callableStatement.execute();

            String resultMessage = callableStatement.getString(2);

            System.out.println("Deletion Verification Result: " + resultMessage);
        }
    }

    private static boolean authenticateUser(Connection connection, String username, String password) throws SQLException {
        String query = "{CALL AuthenticateUser(?, ?, ?)}";
        try (CallableStatement statement = connection.prepareCall(query)) {
            statement.setString(1, username);
            statement.setString(2, password);
            statement.registerOutParameter(3, Types.BIT);
            statement.execute();
            boolean authenticated = statement.getBoolean(3);
            return authenticated;
        }
    }
}
