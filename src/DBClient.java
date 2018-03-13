/**
 * @class CSC371 - Databases
 * @professor Dr. Mooney
 * @author Aaron Wink (Code Overlord), Nahesha Paulection, Stephen Clabaugh
 * @date_modified May 10, 2017
 * @description A client for the group's Mock Facebook Database.
 */

import java.sql.*;
import java.util.*;

public class DBClient {
	static Connection c = null; // The connection to the database.
	static DatabaseMetaData meta = null; // The metadata for the database.
	static int connected = 0; // Variable to tell whether or not the client is
								// connected.
	static Scanner in = null; // The client's user input scanner.

	public static void main(String[] argv) throws SQLException {
		LoadDriver();
		in = new Scanner(System.in);
		meta = c.getMetaData();

		// The main loop for the client; takes user input infinitely until
		// either it crashes or the user quits the program.
		while (connected == 1) {
			System.out.println("\nPlease enter a command:");
			String s = in.nextLine();

			// A series of if/else statements that determines functionality
			// depending on user input.
			if (StringProcessor("delete", s) == true) {
				// Allows the user to delete a row specified by its primary key.
				Delete();
			} else if (StringProcessor("help", s) == true) {
				// Displays commands and their descriptions for the user.
				Help();
			} else if (StringProcessor("insert", s) == true) {
				// Allows the user to insert information into tables.
				Insert();
			} else if (StringProcessor("insert-help", s) == true) {
				// Displays information about a specific table and its attribute
				// columns.
				// Intended to show the user what is needed on INSERT.
				InsertHelp();
			} else if (StringProcessor("show tables", s) == true) {
				// Lists all tables in the DB.
				ShowTables();
			} else if (StringProcessor("query", s) == true) {
				// Allows the user to make a query; the results are displayed.
				Query();
			} else if (StringProcessor("quit", s) == true) {
				// Allows the user to exit the application.
				Quit();
			} else {
				System.out.println("Invalid command; please try again.");
			}
		}
	}

	/**
	 * Prompts the user to select a table to delete a primary key from. The
	 * program then prompts them to select a primary key from the list of all
	 * primary keys in that table. The program then constructs a DELETE
	 * statement from the information obtained from the user and executes it,
	 * deleting the row.
	 *
	 * @throws SQLException
	 */
	private static void Delete() throws SQLException {
		// Allows the user to delete rows from tables.
		System.out.println(
				"---Note!---\nON DELETE CASCADE is active for all keys.\nPrimary key deletions will be automatically cascaded.\n-----------");
		System.out.println("Please specify a table to modify.");
		ShowTables();

		// Retrieves the name of the column that houses the primary keys in the
		// specified table.
		String tableName = in.nextLine();
		ResultSet rs = meta.getPrimaryKeys(null, null, tableName);
		String pkColName = null;
		while (rs.next()) {
			pkColName = rs.getString("COLUMN_NAME");
			// System.out.println(pkColName);
		}

		// Prints out a list of all primary keys in the specified table.
		System.out.println("Please select a primary key from this table to delete the row of.");
		GetResults("SELECT " + pkColName + " FROM " + tableName);

		// Forms and executes the DELETE statement. It then prints out the table
		// post-deletion.
		String pkTarget = in.nextLine();
		String delStatement = "DELETE FROM " + tableName + " WHERE " + pkColName + " = " + pkTarget;
		PreparedStatement ps = c.prepareStatement(delStatement);
		ps.executeUpdate();
		GetResults("SELECT * FROM " + tableName);
	}

	/**
	 * Retrieves the name of the column a foreign key references based on input.
	 *
	 * @param input
	 *            - Either user input or a constructed input. Takes the form of
	 *            an INSERT statement.
	 * @return pkColName - The name of the column of the referenced primary key.
	 * @throws SQLException
	 */
	private static String getColumn(String input) throws SQLException {
		String[] spaceSplit = input.split("\\s");
		ResultSet rs = meta.getImportedKeys(null, null, spaceSplit[2]);
		String pkColName = null;
		while (rs.next()) {
			pkColName = rs.getString("PKCOLUMN_NAME");
		}
		return pkColName;
	}

	/**
	 * Splits input by spaces, then sequentially prints column names, a divider,
	 * and then the values of said columns.
	 *
	 * @param input
	 *            - The user's input; may be an artificial input constructed by
	 *            the client.
	 * @throws SQLException
	 */
	private static void GetResults(String input) throws SQLException {
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery(input);

		String[] spaceSplit = input.split("\\s");

		// Does a check immediately in case * is the only SELECT specifier.
		// Circumvents an exception that arises later if * is left alone.
		if (spaceSplit[1].equals("*")) {
			ResultSetMetaData rsm = rs.getMetaData();
			int colNum = rsm.getColumnCount();

			// Prints out column names.
			System.out.println("Data fields and data types within the table:");
			for (int i = 1; i < colNum; i++) {
				String name = rsm.getColumnName(i);
				System.out.print(name + "\t\t");
			}

			// Prints out the divider.
			System.out.println("");
			for (int k = 0; k < colNum; k++) {
				System.out.print("================");
			}

			// Prints out the data values in each column.
			System.out.println("");
			while (rs.next()) {
				for (int m = 0; m < colNum; m++) {
					String data = rs.getString(rsm.getColumnName(m + 1));
					System.out.print(data + "\t\t");
				}
				System.out.println("");
			}
		}

		// If * is not the only SELECT specifier, commas must then be removed
		// after the space-split.
		for (int i = 0; i < spaceSplit.length; i++) {
			if (i == 1) {
				String[] commaSplit = spaceSplit[1].split(","); // Generates the
																// comma-split
																// String array.

				// Prints out the column names from commaSplit.
				for (int j = 0; j < commaSplit.length; j++) {
					System.out.print(commaSplit[j] + "\t\t");
				}

				// Prints the divider.
				System.out.println("");
				for (int k = 0; k < commaSplit.length; k++) {
					System.out.print("================");
				}

				// Prints the data values from the columns.
				System.out.println("");
				while (rs.next()) {
					for (int m = 0; m < commaSplit.length; m++) {
						String data = rs.getString(commaSplit[m]);
						System.out.print(data + "\t\t");
					}
					System.out.println("");
				}
			}
		}
	}

	/**
	 * Retrieves the name of the table a foreign key references based on input.
	 *
	 * @param input
	 *            - Either user input or a constructed input. Takes the form of
	 *            an INSERT statement.
	 * @return fkTableName - The name of the referenced table.
	 * @throws SQLException
	 */
	private static String getTableName(String input) throws SQLException {
		String[] spaceSplit = input.split("\\s");
		ResultSet rs = meta.getImportedKeys(null, null, spaceSplit[2]);
		String fkTableName = null;
		while (rs.next()) {
			fkTableName = rs.getString("PKTABLE_NAME");
		}
		return fkTableName;
	}

	/**
	 * A basic function to display information about commands.
	 */
	private static void Help() {
		System.out.print("Current commands:\nhelp, insert, show tables, query, quit\n");
		System.out.println("Delete: Allows the user to delete rows from tables");
		System.out.println("Insert: Allows the user to add new values into the database");
		System.out.println("Insert-help: Allows the user to view columns and data types within tables");
		System.out.println("Show tables: Displays the tables that is in the current database");
		System.out.println("Query: Allows the user to write the queries");
		System.out.println("Quit: Gets the user out of the database");
	}

	/**
	 * Allows the user to insert data to a specified table. Takes the user's
	 * input and uses that to execute the update.
	 *
	 * @throws SQLException
	 */
	private static void Insert() throws SQLException {
		System.out.println("Please type your insert statement:");
		String q = in.nextLine();

		try {
			// Under normal circumstances, simply inserts the user's INSERT
			// statement into the database, then prints the modified table.
			Statement is = c.createStatement();
			is.executeUpdate(q);
			String[] spaceSplit = q.split("\\s");
			System.out.println("Data successfully added.");
			GetResults("SELECT * FROM " + spaceSplit[2]); // spaceSplit[2]
															// refers to
															// tableName in
															// INSERT INTO
															// tableName
															// VALUES....
		} catch (SQLException e) {
			// Abnormal circumstances. Specific error codes are given upon
			// failure, so those are used to identify and handle specific cases.
			if (e.getErrorCode() == 1062) { // Error Code 1062 is thrown when
											// the user attempts to insert a
											// duplicate key value.
				System.out.println("You have attempted to insert a duplicate key value.\nPlease revise and try again.");
			} else if (e.getErrorCode() == 1452) { // Error code 1452 is thrown
													// when the user attempts to
													// insert a foreign key
													// value that does not exist
													// in the referenced table.
				String[] spaceSplit = q.split("\\s");
				String ns, Rns;

				ns = spaceSplit[spaceSplit.length - 1].replaceAll("[()]", "");
				Rns = ns.replace("\'", "");
				System.out.println("The value '" + Rns + "' does not exist in the " + getTableName(q)
						+ "table.\nWould you like to create this entry?");
				System.out.println("y/n");
				String fkConf = in.nextLine();

				// If the user opts to add the new foreign key value to the
				// referenced table...
				if (fkConf.equalsIgnoreCase("y")) {
					ResultSet rs = meta.getTables(null, null, "%", null);
					System.out.println("Columns and data types:");
					System.out.println("");

					while (rs.next()) {
						if (StringProcessor(getTableName(q), rs.getString(3)) == true) {
							Statement st = c.createStatement();
							ResultSet res = st.executeQuery("SELECT * FROM " + getTableName(q));
							printCol(res);
						}
					}

					System.out.println("Please type the insert statement for the primary key:");
					String t = in.nextLine();
					Statement it = c.createStatement();
					Statement is = c.createStatement();
					it.executeUpdate(t);
					is.executeUpdate(q);

					System.out.println("Data successfully added.");
					GetResults(t);
					GetResults(q);
				} else {
					// If the user instead opts to not create the new foreign
					// key value in the referenced table, but insists on
					// inserting...
					System.out.println("Would you like to insert anyway?\n(y/n)");
					String inConf = in.nextLine();
					if (inConf.equalsIgnoreCase("y")) {
						// Prints a small list of all extant, valid foreign key
						// values
						System.out.println("Valid values for DIV_NAME are:");
						System.out.println("Div_Name\n========");
						Statement st = c.createStatement();
						ResultSet res = st.executeQuery("SELECT " + getColumn(q) + " FROM " + getTableName(q));
						while (res.next()) {
							System.out.println(res.getString(getColumn(q)));
						}

						Statement sn = c.createStatement();

						// Begins constructing the new insert statement for
						// later processing.
						String newInsert = spaceSplit[0];
						for (int i = 1; i < spaceSplit.length - 1; i++) {
							newInsert = newInsert.concat(" " + spaceSplit[i]);
						}

						System.out.println("\nWould you like to use one of these values?\n(y/n)");
						String divConf = in.nextLine();
						if (divConf.equalsIgnoreCase("y")) {
							System.out.println("Please specify which value you would like to use.");
							String valConf = in.nextLine();
							res.first();
							while (res.next()) {
								if (valConf.equals(res.getString(getColumn(q)))) {
									// Adds the user's foreign key choice to the
									// end of the statement.
									newInsert = newInsert.concat(" '" + valConf + "')");
								}
							}
						} else {
							// If the user decides to not add their own FK or
							// use a pre-existing FK, defaults the FK value to
							// NULL.
							newInsert = newInsert.concat(" NULL)");
						}
						System.out.println(newInsert);
						sn.executeUpdate(newInsert);
						String rm = "SELECT * FROM " + spaceSplit[2];
						GetResults(rm);
					} else {
						System.out.println("INSERT aborted.");
					}
				}
			} else {
				// Occurs with every other mishap with the INSERT statements.
				System.out.println("Invalid INSERT statement.\nPlease revise and try again.");
			}
		}
	}

	/**
	 * Displays all tables in the DB, then prompts the user to select one for
	 * further information. Then, it displays all column names and their data
	 * types.
	 *
	 * @throws SQLException
	 */
	private static void InsertHelp() throws SQLException {
		System.out.println("Please specify a table to view:");
		ShowTables();
		String r = in.nextLine();
		ResultSet rs = meta.getTables(null, null, "%", null);

		while (rs.next()) {
			if (StringProcessor(r, rs.getString(3)) == true) {
				Statement st = c.createStatement();
				ResultSet res = st.executeQuery("SELECT * FROM " + r);
				printCol(res);
			}
		}
	}

	/**
	 * Loads the drive and connect to the server.
	 */
	private static void LoadDriver() {
		System.out.println("Connecting to Facebook database...");

		// Begins by attempting to detect the JDBC driver.
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("MySQL JDBC driver not found. Please download and add to build path, then try again.");
			e.printStackTrace();
			return;
		}

		// Attempts connection to the database.
		try {
			// Uses the DB's login info to connect.
			c = DriverManager.getConnection("jdbc:mysql://db.cs.ship.edu/csc371-41", "csc371-41", "Bomberman1");
		} catch (SQLException e) {
			System.out.println("Failed to connect.");
			return;
		}

		// Allows the user to perform operations on the data in the DB.
		if (c != null) {
			System.out.print(
					"Welcome to Mock Facebook Database!\n  You are now connected to the database.\n  For a list of commands, type 'help' (without single quotes).\n");
			connected = 1;
		} else {
			System.out.println("Failed to connect.");
		}
	}

	/**
	 * Prints out the column names and the data types associated with them.
	 *
	 * @param rs
	 *            - The ResultSet to derive the columns from.
	 * @throws SQLException
	 */
	private static void printCol(ResultSet rs) throws SQLException {
		ResultSetMetaData rsm = rs.getMetaData();
		int colNum = rsm.getColumnCount();

		System.out.println("Data fields and data types within the table:");
		for (int i = 1; i < colNum; i++) {
			String type = rsm.getColumnTypeName(i);
			String name = rsm.getColumnName(i);
			if (i == colNum - 1) {
				System.out.print(name + " : " + type); // Avoids printing an
														// erroneous comma at
														// the end of the
														// listing.
			} else {
				System.out.print(name + " : " + type + ", ");
			}
		}
	}

	/**
	 * Displays all tables currently in the database.
	 *
	 * @throws SQLException
	 */
	private static void ShowTables() throws SQLException {
		System.out.println("Current tables:");
		ResultSet rs = meta.getTables(null, null, "%", null);
		while (rs.next()) {
			System.out.print(rs.getString(3) + " ");
		}
		System.out.println("");
		rs.close();
	}

	/**
	 * Compares the input with the target string.
	 *
	 * @param target
	 *            - The string being compared to.
	 * @param s
	 *            - The string to be compared.
	 * @return true/false.
	 * @throws SQLException
	 */
	private static boolean StringProcessor(String s, String target) throws SQLException {
		if (s.equals(target))
			return true;
		else
			return false;
	}

	/**
	 * Takes user input through the scanner and uses it to make a query to the
	 * DB.
	 */
	private static void Query() {
		System.out.println("Please enter a query.\nPlease omit spaces after commas when separating column names.");
		String q = in.nextLine();
		try {
			GetResults(q);
		} catch (SQLException e) {
			System.out.println("Invalid query; please try again.");
		}
	}

	/**
	 * Closes the connection to the database and closes the application.
	 *
	 * @throws SQLException
	 */
	private static void Quit() throws SQLException {
		c.close();
		c = null;
		connected = 0;
		System.out.println("Goodbye.");
	}
}