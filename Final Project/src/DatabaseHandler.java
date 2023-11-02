import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Locale;

public class DatabaseHandler {

    private final Connection conn;

    public DatabaseHandler(String url, String username, String password) throws SQLException {
        conn = DriverManager.getConnection(url, username, password);
        conn.setAutoCommit(false);
    }

    public ResultSet getRoomsAndRates() throws SQLException {
        String sql =
                "WITH\n" +
                        "    popularities AS (\n" +
                        "        SELECT Room, ROUND(SUM(GREATEST(DATEDIFF(LEAST(Checkout, CURDATE()), GREATEST(CheckIn, DATE_SUB(CURDATE(), INTERVAL 180 DAY))), 0))/180, 2) Popularity\n" +
                        "        FROM gholland.lab7_reservations\n" +
                        "        GROUP BY Room\n" +
                        "    ),\n" +
                        "    checkIns AS (\n" +
                        "        SELECT Room, IFNULL(MIN(Checkout), CURDATE()) NextCheckIn\n" +
                        "        FROM gholland.lab7_reservations\n" +
                        "        WHERE Checkout >= CURDATE()\n" +
                        "        AND CODE NOT IN (\n" +
                        "            SELECT r1.CODE\n" +
                        "            FROM gholland.lab7_reservations r1 JOIN gholland.lab7_reservations r2 USING (Room)\n" +
                        "            WHERE r1.Checkout = r2.CheckIn\n" +
                        "        )\n" +
                        "        GROUP BY Room\n" +
                        "    ),\n" +
                        "    lastStays AS (\n" +
                        "        SELECT Room, DATEDIFF(LastStayCheckout, CheckIn) LastStayDays, LastStayCheckout\n" +
                        "        FROM gholland.lab7_reservations JOIN (\n" +
                        "            SELECT Room, MAX(Checkout) LastStayCheckout\n" +
                        "            FROM gholland.lab7_reservations\n" +
                        "            WHERE Checkout <= CURDATE()\n" +
                        "            GROUP BY Room\n" +
                        "        ) maxCheckouts USING (Room)\n" +
                        "        WHERE Checkout = LastStayCheckout\n" +
                        "    )\n" +
                        "SELECT rooms.*, Popularity, NextCheckIn, LastStayDays, LastStayCheckOut\n" +
                        "FROM gholland.lab7_rooms rooms LEFT JOIN popularities p ON RoomCode = p.Room LEFT JOIN checkIns c ON RoomCode = c.Room LEFT JOIN lastStays l ON RoomCode = l.Room\n" +
                        "ORDER BY Popularity DESC;";
        Statement statement = conn.createStatement();

        return statement.executeQuery(sql);
    }

    public int getMaxCapacity() throws SQLException {
        String sql = "SELECT MAX(maxOcc) maxCapacity FROM gholland.lab7_rooms";
        Statement statement = conn.createStatement();

        ResultSet rs = statement.executeQuery(sql);
        rs.next();
        return rs.getInt("maxCapacity");
    }

    public ResultSet getAvailableRooms(String roomCode, String bedType, String checkIn, String checkOut, String numChildren, String numAdults) throws SQLException {
        String sql =
                "WITH input AS (\n" +
                        "    SELECT ? UserCheckIn, ? UserCheckout\n" +
                        ")\n" +
                        "SELECT *, ROUND(basePrice * (DATEDIFF(UserCheckout, UserCheckIn) + DATEDIFF(ADDDATE(UserCheckout, 1 - DAYOFWEEK(UserCheckout)), ADDDATE(UserCheckIn, 1 - DAYOFWEEK(UserCheckIn))) / 7 * 0.2 + (DAYOFWEEK(UserCheckIn) = 1) * 0.1 - (DAYOFWEEK(UserCheckout) = 1) * 0.1), 2) Cost\n" +
                        "FROM gholland.lab7_rooms, input\n" +
                        "WHERE ? <= maxOcc AND (RoomCode = ? OR ?) AND (bedType = ? OR ?)\n" +
                        "AND NOT EXISTS (\n" +
                        "    SELECT CODE\n" +
                        "    FROM gholland.lab7_reservations\n" +
                        "    WHERE RoomCode = Room AND NOT CheckIn >= UserCheckout AND NOT Checkout <= UserCheckIn\n" +
                        ")";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, checkIn);
        pstmt.setString(2, checkOut);
        pstmt.setString(3, numChildren+numAdults);
        pstmt.setString(4, roomCode);
        pstmt.setBoolean(5, roomCode.equals("Any"));
        pstmt.setString(6, bedType);
        pstmt.setBoolean(7, bedType.equals("Any"));

        return pstmt.executeQuery();
    }

    public ResultSet getSimilarRooms(String roomCode, String bedType, String checkIn, String checkOut, String numChildren, String numAdults) throws SQLException {
        // TODO: Write better SQL
        String sql =
                "WITH input AS (SELECT 'Any' UserRoomCode, true NoRoomPref, 'Any' UserbedType, true NoBedPref, '2021-12-10' UserCheckIn, '2021-12-25' UserCheckout, 4 People) SELECT rooms.*, UserCheckIn CheckIn, UserCheckout Checkout, ROUND(basePrice * (DATEDIFF(UserCheckout, UserCheckIn) + DATEDIFF(ADDDATE(UserCheckout, 1 - DAYOFWEEK(UserCheckout)), ADDDATE(UserCheckIn, 1 - DAYOFWEEK(UserCheckIn))) / 7 * 0.2 + (DAYOFWEEK(UserCheckIn) = 1) * 0.1 - (DAYOFWEEK(UserCheckout) = 1) * 0.1), 2) Cost FROM gholland.lab7_rooms rooms, input WHERE People <= maxOcc AND NOT EXISTS ( SELECT CODE FROM gholland.lab7_reservations WHERE RoomCode = Room AND NOT CheckIn >= UserCheckout AND NOT Checkout <= UserCheckIn) ORDER BY (RoomCode = UserRoomCode OR NoRoomPref) DESC, (bedType = UserbedType OR NoBedPref) DESC ";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, roomCode);
        pstmt.setBoolean(2, roomCode.equals("Any"));
        pstmt.setString(3, bedType);
        pstmt.setBoolean(4, bedType.equals("Any"));
        pstmt.setString(5, checkIn);
        pstmt.setString(6, checkOut);
        pstmt.setString(7, numChildren+numAdults);

        return pstmt.executeQuery();
    }

    public void createReservation(String roomCode, String checkIn, String checkOut, String rate, String lastName, String firstName, String numAdults, String numChildren) throws SQLException {
        String sql = "SELECT MAX(CODE) maxCode FROM gholland.lab7_reservations";
        Statement statement = conn.createStatement();

        ResultSet rs = statement.executeQuery(sql);
        rs.next();

        sql = "INSERT INTO gholland.lab7_reservations (CODE, Room, CheckIn, Checkout, Rate, LastName, FirstName, Adults, Kids) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, rs.getInt("maxCode")+1);
            pstmt.setString(2, roomCode);
            pstmt.setString(3, checkIn);
            pstmt.setString(4, checkOut);
            pstmt.setString(5, rate);
            pstmt.setString(6, lastName.toUpperCase(Locale.ROOT));
            pstmt.setString(7, firstName.toUpperCase(Locale.ROOT));
            pstmt.setString(8, numAdults);
            pstmt.setString(9, numChildren);
            pstmt.execute();
            conn.commit();
        } catch(SQLException e){
            conn.rollback();
            throw e;
        }
    }

    public boolean changeReservation(String changing, String firstName, String lastName, String beginDate, String endDate, String numChildren, String numAdults, String code) throws SQLException {
        String sql;
        if (!beginDate.equals("") || !endDate.equals("")) {
            sql = "SELECT * FROM gholland.lab7_reservations r2\n" +
                    "WHERE ((? > r2.CheckIn AND ? < r2.Checkout) OR (? > r2.CheckIn AND ? < r2.Checkout))\n" +
                    "  AND r2.Room = (\n" +
                    "     SELECT r1.Room FROM gholland.lab7_reservations r1\n" +
                    "     WHERE r1.CODE = ?\n";

            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, beginDate);
            pstmt.setString(2, beginDate);
            pstmt.setString(3, endDate);
            pstmt.setString(4, endDate);
            pstmt.setString(5, code);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) return false;
        }
        switch (changing) {
            case "1" -> {
                sql = "UPDATE gholland.lab7_reservations SET firstname = ? WHERE code = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, firstName);
                    pstmt.setString(2, code);
                    pstmt.executeUpdate();
                    conn.commit();
                    System.out.println("Successfully changed first name to " + firstName);
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
            case "2" -> {
                sql = "UPDATE gholland.lab7_reservations SET lastname = ? WHERE code = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, lastName);
                    pstmt.setString(2, code);
                    pstmt.executeUpdate();
                    conn.commit();
                    System.out.println("Successfully changed last name to " + lastName);
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
            case "3" -> {
                sql = "UPDATE gholland.lab7_reservations SET checkIn = ? WHERE code = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, beginDate);
                    pstmt.setString(2, code);
                    pstmt.executeUpdate();
                    conn.commit();
                    System.out.println("Successfully changed check in date to to " + beginDate);
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
            case "4" -> {
                sql = "UPDATE gholland.lab7_reservations SET checkOut = ? WHERE code = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, endDate);
                    pstmt.setString(2, code);
                    pstmt.executeUpdate();
                    conn.commit();
                    System.out.println("Successfully changed check out date to " + endDate);
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
            case "5" -> {
                sql = "UPDATE gholland.lab7_reservations SET kids = ? WHERE code = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, numChildren);
                    pstmt.setString(2, code);
                    pstmt.executeUpdate();
                    conn.commit();
                    System.out.println("Successfully changed number of children to " + numChildren);
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
            case "6" -> {
                sql = "UPDATE gholland.lab7_reservations SET adults = ? WHERE code = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, numAdults);
                    pstmt.setString(2, code);
                    pstmt.executeUpdate();
                    conn.commit();
                    System.out.println("Successfully changed number of adults to " + numAdults);
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
        }
        return true;
    }

    public void cancelReservation(String code) throws SQLException {
        String sql = "DELETE FROM gholland.lab7_reservations WHERE Code = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, code);
            pstmt.execute();
            conn.commit();
        } catch(SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    public ResultSet getMatchingReservations(String firstName, String lastName, String beginDate, String endDate, String roomCode, String reservationCode) throws SQLException {
        String sql =
                "SELECT CODE, Room, RoomName, CheckIn, Checkout, LastName, FirstName, Rate, Adults, Kids, maxOcc\n" +
                        "FROM gholland.lab7_reservations reservations JOIN gholland.lab7_rooms rooms ON Room = RoomCode\n" +
                        "WHERE (FirstName LIKE ? OR ?)\n" +
                        "  AND (LastName LIKE ? OR ?)\n" +
                        "  AND (CheckIn = ? OR ?)\n" +
                        "  AND (Checkout = ? OR ?)\n" +
                        "  AND (RoomCode LIKE ? OR ?)\n" +
                        "  AND (CODE LIKE ? OR ?)\n;";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setString(1, firstName);
        pstmt.setBoolean(2, firstName.equals("Any") || firstName.equals(""));
        pstmt.setString(3, lastName);
        pstmt.setBoolean(4, lastName.equals("Any") || lastName.equals(""));
        pstmt.setString(5, beginDate);
        pstmt.setBoolean(6, beginDate.equals("Any") || beginDate.equals(""));
        pstmt.setString(7, endDate);
        pstmt.setBoolean(8, endDate.equals("Any") || endDate.equals(""));
        pstmt.setString(9, roomCode);
        pstmt.setBoolean(10, roomCode.equals("Any") || roomCode.equals(""));
        pstmt.setString(11, reservationCode);
        pstmt.setBoolean(12, reservationCode.equals("Any") || reservationCode.equals(""));

        return pstmt.executeQuery();
    }

    public ResultSet getRevenue() throws SQLException {
        String sql =
                "WITH revenues AS (\n" +
                        "    SELECT \n" +
                        "        Room,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-02-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-01-01'))), 0))) January,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-03-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-02-01'))), 0))) February,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-04-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-03-01'))), 0))) March,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-05-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-04-01'))), 0))) April,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-06-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-05-01'))), 0))) May,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-07-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-06-01'))), 0))) June,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-08-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-07-01'))), 0))) July,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-09-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-08-01'))), 0))) August,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-10-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-09-01'))), 0))) September,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-11-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-10-01'))), 0))) October,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE()), '-12-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-11-01'))), 0))) November,\n" +
                        "        ROUND(SUM(Rate * GREATEST(DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE())+1, '-01-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-12-01'))), 0))) December,\n" +
                        "        ROUND(SUM(Rate * DATEDIFF(LEAST(Checkout, CONCAT(YEAR(CURDATE())+1, '-01-01')), GREATEST(CheckIn, CONCAT(YEAR(CURDATE()), '-01-01'))))) Total\n" +
                        "    FROM gholland.lab7_reservations\n" +
                        "    WHERE YEAR(CURDATE()) BETWEEN YEAR(CheckIn) AND YEAR(Checkout)\n" +
                        "    GROUP BY Room\n" +
                        ")\n" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    (SELECT * FROM revenues) \n" +
                        "    UNION ALL \n" +
                        "    (SELECT 'All Rooms', SUM(January), SUM(February), SUM(March), SUM(April), SUM(May), SUM(June), SUM(July), SUM(August), SUM(September), SUM(October), SUM(November), SUM(December), SUM(Total) FROM revenues)\n" +
                        ") result\n";
        Statement statement = conn.createStatement();

        return statement.executeQuery(sql);
    }
}
