import java.sql.ResultSetMetaData;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Map;
import java.sql.SQLException;
import java.sql.ResultSet;

public class InnReservations {

    public static void main(String[] args) {
        if (setup() == 1) {
            System.out.println("Setup failed");
            System.exit(1);
        }

        System.out.println("Welcome to our inn!");
        System.out.println();

        mainMenuShell();

        System.out.println();
        System.out.println();
        System.out.println("Good bye, come again soon!");
    }

    private static int setup() {
        String url = System.getenv(URL);
        if (url == null) {
            System.out.println("Environment variable " + URL + " not defined");
            return 1;
        }
        String username = System.getenv(USERNAME);
        if (username == null) {
            System.out.println("Environment variable " + USERNAME + " not defined");
            return 1;
        }
        String password = System.getenv(PASSWORD);
        if (password == null) {
            System.out.println("Environment variable " + PASSWORD + " not defined");
            return 1;
        }

        try {
            dbHandler = new DatabaseHandler(url, username, password);
        } catch (SQLException e) {
            System.out.println("Connection to database unsuccessful. Are all of the connection variables the correct values?");
            return 1;
        }

        return 0;
    }

    private static void mainMenuShell() {
        displayPrompt();
        while (inputScanner.hasNextLine()) {
            String input = inputScanner.nextLine();
            System.out.println();

            executeCommand(input);
            System.out.println();

            displayPrompt();
        }
    }

    private static void displayPrompt() {
        System.out.println("Choose an option by entering a number (1-6):");
        System.out.println("  1) Rooms and Rates");
        System.out.println("  2) Reservations");
        System.out.println("  3) Reservation Change");
        System.out.println("  4) Reservation Cancellation");
        System.out.println("  5) Detailed Reservation Information");
        System.out.println("  6) Revenue");
        System.out.println();
        System.out.print("Option: ");
    }

    private static void executeCommand(String option) {
        try {
            switch (option) {
                case "1" -> roomsAndRates();
                case "2" -> reservations();
                case "3" -> reservationChange();
                case "4" -> reservationCancellation();
                case "5" -> detailedReservationInformation();
                case "6" -> revenue();
                default -> System.out.println("Unrecognized option. Make sure to select a number from 1-6");
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
        }
    }

    private static void roomsAndRates() throws SQLException {
        ResultSet rs = dbHandler.getRoomsAndRates();

        System.out.println("Rooms and Rates:");
        output(rs, false, 13);
    }

    private static void reservations() throws SQLException {
        System.out.println("Please enter the following reservation information");
        System.out.print("First name: ");
        String firstName = inputScanner.nextLine();
        System.out.print("Last name: ");
        String lastName = inputScanner.nextLine();
        System.out.print("Room code (\"Any\" for no preference): ");
        String roomCode = inputScanner.nextLine();
        System.out.print("Desired bed type (\"Any\" for no preference): ");
        String bedType = inputScanner.nextLine();
        System.out.print("Check-in date: ");
        String checkIn = inputScanner.nextLine();
        System.out.print("Check-out date: ");
        String checkOut = inputScanner.nextLine();
        System.out.print("Number of children: ");
        String numChildren = inputScanner.nextLine();
        System.out.print("Number of adults: ");
        String numAdults = inputScanner.nextLine();
        System.out.println();

        if (Integer.parseInt(numChildren) + Integer.parseInt(numAdults) > dbHandler.getMaxCapacity()) {
            System.out.println("Number of occupants too large, no suitable rooms available");
            return;
        }

        ResultSet rs = dbHandler.getAvailableRooms(roomCode, bedType, checkIn, checkOut, numChildren, numAdults);
        if (!rs.isBeforeFirst()) {
            System.out.println("No available rooms matching your preferences exactly. Searching for similar rooms");
            rs = dbHandler.getSimilarRooms(roomCode, bedType, checkIn, checkOut, numChildren, numAdults);
        }

        System.out.println("Available rooms:");
        output(rs, true, 12);
        System.out.print("Choose an option by entering a valid number, or enter anything else to cancel: ");

        String optionStr = inputScanner.nextLine();
        try {
            Integer.parseInt(optionStr);
        } catch (NumberFormatException e) {
            return;
        }
        int option = Integer.parseInt(optionStr);
        if (option < 1 || option > rs.getRow()) {
            return;
        }

        rs.absolute(option);
        System.out.println();
        System.out.println("Preview of your reservation:");
        System.out.printf("%s %s\n", firstName, lastName);
        System.out.printf("%s, %s, %s beds\n", rs.getString("RoomCode"), rs.getString("RoomName"), rs.getString("bedType"));
        System.out.printf("%s through %s\n", checkIn, checkOut);
        System.out.printf("%s adults\n", numAdults);
        System.out.printf("%s children\n", numChildren);
        System.out.printf("Costing $%.2f\n", rs.getFloat("Cost"));
        System.out.println();

        System.out.print("Confirm? y/n: ");
        String confirm = inputScanner.nextLine();
        if (!confirm.equals("y")) {
            System.out.println("Reservation cancelled, returning to main menu");
            return;
        }
        dbHandler.createReservation(rs.getString("RoomCode"), checkIn, checkOut, rs.getString("basePrice"), lastName, firstName, numAdults, numChildren);
        System.out.println("Reservation created");
    }

    private static void reservationChange() throws SQLException {
        String firstName = "";
        String lastName = "";
        String beginDate = "";
        String endDate = "";
        String numChildren = "";
        String numAdults = "";
        System.out.print("Please enter your reservation code to change reservation information: ");
        String code = inputScanner.nextLine();
        System.out.println("\nSelect what you would like to choose within your reservation.");
        System.out.println("  1) First Name");
        System.out.println("  2) Last Name");
        System.out.println("  3) Begin Date");
        System.out.println("  4) End Date");
        System.out.println("  5) Number of Children");
        System.out.println("  6) Number of Adults");
        System.out.print("Choose an option by entering a valid number: ");
        String changing = inputScanner.nextLine();
        switch (changing) {
            case "1" -> {
                System.out.print("Please enter what you would like the new first name to be or (0) to exit: ");
                firstName = inputScanner.nextLine();
            }
            case "2" -> {
                System.out.print("Please enter what you would like the new last name to be or (0) to exit: ");
                lastName = inputScanner.nextLine();
            }
            case "3" -> {
                System.out.print("Please enter what you would like the new check in date (YYYY-MM-DD) to be or (0) to exit: ");
                beginDate = inputScanner.nextLine();
            }
            case "4" -> {
                System.out.print("Please enter what you would like the new check out date (YYYY-MM-DD) to be or (0) to exit: ");
                endDate = inputScanner.nextLine();
            }
            case "5" -> {
                System.out.print("Please enter what you would like the new number of children to be or (0) to exit: ");
                numChildren = inputScanner.nextLine();
            }
            case "6" -> {
                System.out.print("Please enter what you would like the new number of adults to be or (0) to exit: ");
                numAdults = inputScanner.nextLine();
            }
        }
        if (firstName.equals("0") || lastName.equals("0") || beginDate.equals("0") || endDate.equals("0") || numChildren.equals("0") || numAdults.equals("0")) {
            return;
        }
        if (dbHandler.changeReservation(changing, firstName, lastName, beginDate, endDate, numChildren, numAdults, code)) {
            System.out.println("Reservation successfully changed");
        } else {
            System.out.println("New requested dates conflict with existing reservations, try again");
        }
    }

    private static void reservationCancellation() throws SQLException {
        System.out.print("\nPlease enter your reservation code: ");
        String code = inputScanner.nextLine();
        System.out.print("\nAre you sure you wish to cancel reservation? Please re-enter reservation code to confirm: ");
        String newCode = inputScanner.nextLine();
        if(!code.equals(newCode)) {
            System.out.println("Reservation codes do not match. Returning to main menu");
            return;
        }

        dbHandler.cancelReservation(code);
        System.out.println("\nReservation successfully cancelled. Come again.");
    }

    private static void detailedReservationInformation() throws SQLException {
        System.out.println("Please enter the following reservation search fields:");
        System.out.print("First name (\"Any\" or blank for no preference): ");
        String firstName = inputScanner.nextLine();
        System.out.print("Last name (\"Any\" or blank for no preference): ");
        String lastName = inputScanner.nextLine();
        System.out.print("Beginning date (YYYY-MM-DD) (\"Any\" or blank for no preference): ");
        String beginDate = inputScanner.nextLine();
        System.out.print("End date (YYYY-MM-DD) (\"Any\" or blank for no preference): ");
        String endDate = inputScanner.nextLine();
        System.out.print("Room code (\"Any\" or blank for no preference): ");
        String roomCode = inputScanner.nextLine();
        System.out.print("Reservation code (\"Any\" or blank for no preference): ");
        String reservationCode = inputScanner.nextLine();

        ResultSet rs = dbHandler.getMatchingReservations(firstName,
                lastName,
                beginDate,
                endDate,
                roomCode,
                reservationCode);

        System.out.println("\nMatching Reservation Information:");
        output(rs, false, 12);
    }

    private static void revenue() throws SQLException {
        ResultSet rs = dbHandler.getRevenue();

        System.out.println("Revenue by Room: ");
        output(rs, false, 12);
    }

    private static void output(ResultSet rs, boolean numbered, int default_column_padding) throws SQLException {
        rs.beforeFirst();
        ResultSetMetaData rsmd = rs.getMetaData();
        ArrayList<String> columns = new ArrayList<>();
        for (int i = 1; i <= rsmd.getColumnCount(); ++i)
            columns.add(rsmd.getColumnName(i));
        if (numbered)
            System.out.print(" ".repeat(NUMBERING_PADDING));
        for (String column : columns)
            System.out.format("%-"+getColumnPadding(column, default_column_padding)+"s", column);
        System.out.println();
        if (numbered)
            System.out.print("-".repeat(NUMBERING_PADDING));
        for (String column : columns)
            System.out.print("-".repeat(getColumnPadding(column, default_column_padding)));
        System.out.println();
        while (rs.next()) {
            if (numbered) {
                String rowNum = rs.getRow() + ")";
                System.out.format("%-"+NUMBERING_PADDING+"s", rowNum);
            }
            for (String column : columns) {
                System.out.format("%-"+getColumnPadding(column, default_column_padding)+"s", rs.getString(column));
            }
            System.out.println();
        }
        rs.previous();
        if (!rs.isBeforeFirst()) {
            if (numbered)
                System.out.print(" ".repeat(NUMBERING_PADDING));
            System.out.println("<No results>");
        }
    }

    private static int getColumnPadding(String column, int default_column_padding) {
        Integer padding = COLUMN_PADDINGS.get(column);

        return padding == null ? default_column_padding : padding;
    }
}