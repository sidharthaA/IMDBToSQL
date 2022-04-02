package edu.rit.ibd.a1;

import com.sun.jdi.IntegerValue;
import com.sun.jdi.connect.Connector;

import java.awt.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

public class IMDBFilesToMySQL {


	public static void main(String[] args) throws Exception {

		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String folderToIMDBGZipFiles = args[3];

		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);

		HashSet<Integer> movieHashSet = new HashSet<>();
		System.out.println("my code starts");
		// TODO 0: Your code here!


		// Before inserting data, you need to create a relation. You can create all relations at once or while you are
		//		populating the database. Creating a new relation is as follows:
		//			st = con.prepareStatement("CREATE TABLE X...");
		//			st.execute();
		//			st.close();
		//
		// IMPORTANT: You should never ever use schema names such as "CREATE TABLE sch.X..." If you do so, you are forcing
		//	a database named 'sch' to exist, which defeats the purpose of receiving the database as an input parameter.
		// I REPEAT: NEVER USE SCHEMA NAMES IN THIS COURSE!!!!!
		//
		// In this assignment, you will learn how to load data from external files into a relational database using JDBC.
		//	In general, these external files will not be perfect and contain data issues. When dealing with a relational
		//	database, the most important of these issues are the foreign keys: a piece of data may not be available in a
		//	referenced relation. This actually happens in the IMDB data so, in order to avoid these issues, you must NOT
		//	create foreign keys. Instead, you need to create the relations without any constraints except for primary
		//	keys. At the end of the process, you must delete all the data that is not relevant to us, i.e., tuples that
		//	point to movies or people that are not present in the tables must be deleted.
		//
		// It is very important to release resources ASAP, so always close everything as soon as you are done.
		// While debugging, you may want to delete a previously create relation, you can do so as follows:
		//			st = con.prepareStatement("DROP TABLE IF EXISTS [TABLE_NAME]");
		//
		// Be careful with this statement as relations will be completely removed, including the data.
		// Note that, if you have create foreign keys, you cannot drop a relation that references another, that is,
		//	to drop a relation you may first to drop other relation(s) first.
		//
		// Assuming a relation has been created, you can insert data by reading from the GZip files as follows:
		//			InputStream gzipStream = new GZIPInputStream(
		//				new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));
		//			Scanner sc = new Scanner(gzipStream, "UTF-8");
		//			while (sc.hasNextLine()) {
		//				String currentLine = sc.nextLine();
		//				// Do your amazing stuff.
		//			}
		//			sc.close();
		//
		// To load data massively, you should use the batch command. First, the JDBC URL should have a parameter as follows:
		//	?rewriteBatchedStatements=true. You should not worry about this since the grading software will take care of it.
		//	Then, create an INSERT statement as follows:
		//			st = con.prepareStatement("INSERT INTO ...");
		//
		// There are different options to load the data but the recommended one is to rely on parameterized queries:
		//			st = con.prepareStatement("INSERT INTO X(a, b, c) VALUES (?,?,?)");
		//
		// Now, you can use st.set[Int/String/Float...](i, v), where i denotes the position of the parameter starting in 1,
		//	and v denotes the specific value. You need to use the appropriate method according to the type of the attribute
		//	in the relation.
		// Once you are doing filling the data for this specific tuple, you must add the statement to the batch:
		//			st.addBatch();
		//
		// This will internally add something like "INSERT INTO X(a, b, c) VALUES (1,'t','x')" to be executed at a later
		//	stage.
		//
		// Remember that, by default, every statement will be executed in isolation, which defeats our purpose of running
		//	multiple statements at a time. To disable such behavior, you must explicitly invoke the method in the connection:
		//			con.setAutoCommit(false);
		//
		// After this method is executed, changes in the database will not be reflected unless you call con.commit().
		//
		// All these statements will be stored in main memory. Since there are restrictions on the amount of main memory you
		//	can use, you must commit from time to time to release memory. I recommend to set a "step" variable that will
		//	control how many statements you will process at a time as follows:
		//			int step = 1000;
		//			(...)
		//			int cnt = 0;
		//			while (...) {
		//				cnt++;
		//				// Keep working on adding statements to the batch.
		//				(...)
		//				if (cnt % 1000 == 0) {
		//					// Execute all pending statements.
		//					st.executeBatch();
		//					// Commit the changes.
		//					con.commit();
		//					// There are no more statements, new statements will be added.
		//				}
		//			}
		//			// Leftovers!
		//			st.executeBatch();
		//			con.commit();
		//			st.close();
		//
		// Note that the last calls after the loop are necessary to deal with the leftover statements. The size of the step will determine the amount of memory
		//	you will use. You will generally achieve better performance by handling many statements at once, but you can use more memory than allowed. You can
		//	play around to find the best configuration, but remember that your programs must work on an external computer that you do not have access to, so
		//	it is generally better to play safe without pushing the limit.
		//
		// Finally, there are certain tuples that are repeated. You should use "INSERT IGNORE INTO..." to ignore issues with primary keys. The first tuple will
		//	be kept while the rest, duplicates will be ignored without errors.

		con.setAutoCommit(false);
		PreparedStatement st = con.prepareStatement("DROP TABLE IF EXISTS Actor");
		st.execute();
		st.close();

		st = con.prepareStatement("DROP TABLE IF EXISTS Director");
		st.execute();
		st.close();

		st = con.prepareStatement("DROP TABLE IF EXISTS Movie");
		st.execute();
		st.close();

		st = con.prepareStatement("DROP TABLE IF EXISTS Producer");
		st.execute();
		st.close();

		st = con.prepareStatement("DROP TABLE IF EXISTS Writer");
		st.execute();
		st.close();

		st = con.prepareStatement("DROP TABLE IF EXISTS Person");
		st.execute();
		st.close();

		st = con.prepareStatement("DROP TABLE IF EXISTS MovieGenre");
		st.execute();
		st.close();

		st = con.prepareStatement("DROP TABLE IF EXISTS Genre");
		st.execute();
		st.close();
		con.commit();





		st = con.prepareStatement("CREATE TABLE Actor(pid INTEGER, mid INTEGER, PRIMARY KEY (pid,mid))");
		st.execute();
		st.close();

		st = con.prepareStatement("CREATE TABLE Director(pid INTEGER, mid INTEGER, PRIMARY KEY (pid,mid))");
		st.execute();
		st.close();

		st = con.prepareStatement("CREATE TABLE Producer(pid INTEGER, mid INTEGER, PRIMARY KEY (pid,mid))");
		st.execute();
		st.close();

		st = con.prepareStatement("CREATE TABLE Person(id INTEGER, name VARCHAR(150), birthYear INTEGER(4)," +
				"deathYear INTEGER(4), PRIMARY KEY (id))");
		st.execute();
		st.close();

		st = con.prepareStatement("CREATE TABLE Movie(id INTEGER, title VARCHAR(300), isAdult BOOLEAN," +
				"year INTEGER, runtime INTEGER, rating FLOAT, votes INTEGER, PRIMARY KEY (id))");
		st.execute();
		st.close();

		st = con.prepareStatement("CREATE TABLE Genre(id INTEGER, name VARCHAR(150) UNIQUE, PRIMARY KEY (id))");
		st.execute();
		st.close();

		st = con.prepareStatement("CREATE TABLE Writer(pid INTEGER, mid INTEGER, PRIMARY KEY (pid,mid))");
		st.execute();
		st.close();

		st = con.prepareStatement("CREATE TABLE MovieGenre(mid INTEGER, gid INTEGER, PRIMARY KEY (mid, gid))");
		st.execute();
		st.close();
		con.commit();





		st = con.prepareStatement("INSERT INTO Movie(id, title, isAdult, year, runtime, rating, votes) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?)");
//		PreparedStatement st_genre = con.prepareStatement("INSERT INTO Genre(id, name) VALUES(?, ?)");
		PreparedStatement st_movieGenre = con.prepareStatement("INSERT INTO MovieGenre(mid, gid) VALUES(?, ?)");


		int countLines = 0;
		InputStream gzipStream = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"));
		Scanner sc = new Scanner(gzipStream, "UTF-8");

		List<String> genreList = new ArrayList<>();
		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();
			countLines++;

			String[] splitLine = currentLine.split("\t");
			if (splitLine[1].equals("movie") || splitLine[1].equals("tvMovie")) {
				movieHashSet.add(Integer.valueOf(splitLine[0].replace("tt", "")));
				// Movie ID
				st.setInt(1, Integer.valueOf(splitLine[0].replace("tt", "")));
				// Movie title
				st.setString(2, splitLine[3]);
				// Movie is adult
				st.setBoolean(3, splitLine[4].equals("1"));
				// Movie runtime (minutes)
				if (!splitLine[7].equals("\\N")) {
					st.setInt(5, Integer.valueOf(splitLine[7]));
				} else {
					st.setNull(5, Types.INTEGER);
				}
				// Movie year
				try {
					st.setInt(4, Integer.valueOf(splitLine[5]));
				} catch (Throwable oops) {
					st.setNull(4, Types.INTEGER);
				}
				// rating (default null)
				st.setNull(6, Types.INTEGER);
				// votes (default null)
				st.setNull(7, Types.INTEGER);

				st.addBatch();

				// adding data into Genre and MovieGenre tables
				String[] splitComma = splitLine[8].split(",");
				for (String csv : splitComma) {
					if (!csv.equals("\\N")) {
						if (!genreList.contains(csv)) {
							genreList.add(csv);
							PreparedStatement st_genre = con.prepareStatement("INSERT INTO Genre(id,name) VALUES(?,?)");
							st_genre.setInt(1, genreList.indexOf(csv) + 1);
							st_genre.setString(2, csv);
							st_genre.addBatch();
							st_genre.executeBatch();
							con.commit();
						}
						st_movieGenre.setInt(1, Integer.parseInt(splitLine[0].substring(2)));
						st_movieGenre.setInt(2, genreList.indexOf(csv)+1);
						st_movieGenre.addBatch();
					}
				}
			}

			if (countLines % 2000 == 0) {
				st.executeBatch();
				con.commit();
				st_movieGenre.executeBatch();
			}
		}

		st.executeBatch();
		st_movieGenre.executeBatch();
		con.commit();
		st.close();
		st_movieGenre.close();
		sc.close();
		gzipStream.close();

		// Update Rating and Votes column of movie table
		st = con.prepareStatement("UPDATE Movie SET rating = ?, votes = ? WHERE ID = ? ");

		countLines = 0;
		gzipStream = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "title.ratings.tsv.gz"));
		sc = new Scanner(gzipStream, "UTF-8");
		sc.nextLine();
		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();
			countLines++;

			String[] splitLine = currentLine.split("\t");
			// Updating ratings column
			st.setInt(2, Integer.valueOf(splitLine[2]));
			st.setFloat(1, Float.valueOf(splitLine[1]));
			st.setInt(3, Integer.valueOf(splitLine[0].replace("tt", "")));
			st.executeUpdate();
			st.addBatch();

			if (countLines % 1000 == 0) {
				System.out.println(new Date() + " movie update--Processed so far: " + countLines);
				st.executeBatch();
				con.commit();
			}

		}
		st.executeBatch();
		con.commit();
		st.close();
		sc.close();
		gzipStream.close();

		// Populating Person Table
		st = con.prepareStatement("INSERT INTO Person(id, name, birthYear, deathYear) VALUES (?, ?, ?, ?)");

		countLines = 0;
		gzipStream = new GZIPInputStream(
				new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
		sc = new Scanner(gzipStream, "UTF-8");

		sc.nextLine();
		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();
			countLines++;
			String[] splitLine = currentLine.split("\t");
			// name id
			st.setInt(1, Integer.valueOf(splitLine[0].replace("nm", "")));
			// name
			st.setString(2, splitLine[1]);
			// birth year
			try {
				st.setInt(3, Integer.valueOf(splitLine[2]));
			} catch (Throwable oops) {
				st.setNull(3, Types.INTEGER);
			}
			// death year
			try {
				st.setInt(4, Integer.valueOf(splitLine[3]));
			} catch (Throwable oops) {
				st.setNull(4, Types.INTEGER);
			}

			st.addBatch();

			if (countLines % 1000 == 0) {
				st.executeBatch();
				con.commit();
			}
		}

		st.executeBatch();
		sc.close();
		con.commit();
		st.close();

		PreparedStatement directorST = con.prepareStatement("INSERT IGNORE INTO Director(pid, mid) VALUES(?,?)");
		PreparedStatement writerST = con.prepareStatement("INSERT IGNORE INTO Writer(pid, mid) VALUES(?,?)");
		PreparedStatement producerST = con.prepareStatement("INSERT IGNORE INTO Producer(pid, mid) VALUES(?,?)");
		PreparedStatement actorST = con.prepareStatement("INSERT IGNORE INTO Actor(pid, mid) VALUES(?,?)");


		countLines = 0;
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.crew.tsv.gz"));
		sc = new Scanner(gzipStream, "UTF-8");
		sc.nextLine();

		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();
			countLines++;
			String[] splitLine = currentLine.split("\t");
			String[] commaSeparatedDirector = splitLine[1].split(",");

			String[] commaSeparatedWriter = splitLine[2].split(",");

			if (!splitLine[1].equals("\\N") && movieHashSet.contains(Integer.valueOf(splitLine[0].substring(2)))) {
				for (String csv_Director : commaSeparatedDirector) {
					directorST.setInt(1, Integer.valueOf(csv_Director.replace("nm", "")));
					directorST.setInt(2, Integer.valueOf(splitLine[0].replace("tt", "")));
					directorST.addBatch();
				}
			}
			if (!splitLine[2].equals("\\N") && movieHashSet.contains(Integer.valueOf(splitLine[0].substring(2)))) {
				for (String csv_Writer : commaSeparatedWriter) {
					writerST.setInt(1, Integer.valueOf(csv_Writer.replace("nm", "")));
					writerST.setInt(2, Integer.valueOf(splitLine[0].replace("tt", "")));
					writerST.addBatch();
				}
			}

			if (countLines % 1000 == 0) {
				directorST.executeBatch();
				writerST.executeBatch();
				con.commit();
			}
		}
		directorST.executeBatch();
		writerST.executeBatch();
		sc.close();
		con.commit();

		// Processing title.principals.tsv.gz file to populate Producer; Writer; Director and Actor tables
		countLines = 0;
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
		sc = new Scanner(gzipStream, "UTF-8");
		sc.nextLine();
		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();
			countLines++;
			String[] splitLine = currentLine.split("\t");
			String[] commaSeparatedDirAndWriter = splitLine[3].split(",");
			String[] commaSeparatedPID = splitLine[2].split(",");

			if (!splitLine[3].equals("\\N")) {
				for (String csv : commaSeparatedDirAndWriter) {
					for (String cpid : commaSeparatedPID) {
						if (movieHashSet.contains(Integer.valueOf(splitLine[0].replace("tt", ""))) /*&& personHashSet.contains(Integer.valueOf(csv.replace("nm", "")))*/) {

							//Populating Director table
							if (csv.equalsIgnoreCase("director")) {
								directorST.setInt(1, Integer.valueOf(cpid.replace("nm", "")));
								directorST.setInt(2, Integer.valueOf(splitLine[0].replace("tt", "")));
								directorST.addBatch();

							//Populating Writer table
							} else if (csv.equalsIgnoreCase("writer")) {
//
								writerST.setInt(1, Integer.valueOf(cpid.replace("nm", "")));
								writerST.setInt(2, Integer.valueOf(splitLine[0].replace("tt", "")));
								actorST.addBatch();

							// Populating Producer table
							} else if (csv.equalsIgnoreCase("producer")) {

								producerST.setInt(1, Integer.valueOf(cpid.replace("nm", "")));
								producerST.setInt(2, Integer.valueOf(splitLine[0].replace("tt", "")));
								producerST.addBatch();
							}

							// Populating Actor table
							if (csv.contains("self") || csv.contains("act")) {

								actorST.setInt(1, Integer.valueOf(cpid.replace("nm", "")));
								actorST.setInt(2, Integer.valueOf(splitLine[0].replace("tt", "")));
								actorST.addBatch();
							}
						}
					}
				}
			}
			if (countLines % 1000 == 0) {
				actorST.executeBatch();
				directorST.executeBatch();
				producerST.executeBatch();
				writerST.executeBatch();
				con.commit();
			}
		}

		directorST.executeBatch();
		writerST.executeBatch();
		producerST.executeBatch();
		actorST.executeBatch();

		sc.close();
		con.commit();
		directorST.close();
		writerST.close();
		producerST.close();
		actorST.close();

		// Deleting redundant data from Person table

		PreparedStatement stDelete = con.prepareStatement("DELETE from Director where pid not in (SELECT id from Person)");
		stDelete.executeUpdate();
		stDelete.close();
		con.commit();
		stDelete = con.prepareStatement("DELETE from Producer where pid not in (SELECT id from Person)");
		stDelete.executeUpdate();
		stDelete.close();
		con.commit();
		stDelete = con.prepareStatement("DELETE from Actor where pid not in (SELECT id from Person)");
		stDelete.executeUpdate();
		stDelete.close();
		con.commit();
		stDelete = con.prepareStatement("DELETE from Writer where pid not in (SELECT id from Person)");
		stDelete.executeUpdate();
		stDelete.close();
		con.commit();

		con.close();

		System.out.println("my code ends");

	}
}
		// TODO 0: End of your code.
