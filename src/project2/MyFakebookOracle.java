package project2;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "crowella.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}
	
	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() { 
		
		// Scrollable result set allows us to read forward (using next())
		// and also backward.  
		// This is needed here to support the user of isFirst() and isLast() methods,
		// but in many cases you will not need it.
		// To create a "normal" (unscrollable) statement, you would simply call
		// Statement stmt = oracleConnection.createStatement();
		//
	    try (Statement stmt = 
		 oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
						  ResultSet.CONCUR_READ_ONLY)) {
		
		// For each month, find the number of friends born that month
		// Sort them in descending order of count
		ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from "+
				userTableName+
				" where month_of_birth is not null group by month_of_birth order by 1 desc");
		
		this.monthOfMostFriend = 0;
		this.monthOfLeastFriend = 0;
		this.totalFriendsWithMonthOfBirth = 0;
		
		// Get the month with most friends, and the month with least friends.
		// (Notice that this only considers months for which the number of friends is > 0)
		// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
		//
		while(rst.next()) {
			int count = rst.getInt(1);
			int month = rst.getInt(2);
			if (rst.isFirst())
				this.monthOfMostFriend = month;
			if (rst.isLast())
				this.monthOfLeastFriend = month;
			this.totalFriendsWithMonthOfBirth += count;
		}
		
		// Get the names of friends born in the "most" month
		rst = stmt.executeQuery("select user_id, first_name, last_name from "+
				userTableName+" where month_of_birth="+this.monthOfMostFriend);
		while(rst.next()) {
			Long uid = rst.getLong(1);
			String firstName = rst.getString(2);
			String lastName = rst.getString(3);
			this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Get the names of friends born in the "least" month
		rst = stmt.executeQuery("select first_name, last_name, user_id from "+
				userTableName+" where month_of_birth="+this.monthOfLeastFriend);
		while(rst.next()){
			String firstName = rst.getString(1);
			String lastName = rst.getString(2);
			Long uid = rst.getLong(3);
			this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
		} catch (SQLException err) {
		System.err.println(err.getMessage());
	    }
	}

	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest first name (if there is a tie, include all in result)
	// (2) The shortest first name (if there is a tie, include all in result)
	// (3) The most common first name, and the number of times it appears (if there
	//      is a tie, include all in result)
	//
	public void findNameInfo()  { // Query1
        // Find the following information from your database and store the information as shown

		try (Statement stmt =
					 oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
			// get the longest first name
			String sql = "SELECT DISTINCT length(first_name) AS length, first_name from " + userTableName +
					" WHERE first_name is not NULL " +
					" ORDER by 1 DESC";
			ResultSet rst = stmt.executeQuery(sql);

			int longestLength = 0;
			int shortestLength = Integer.MAX_VALUE;

			if (rst.first()) {
				longestLength = rst.getInt(1);
			}
			if (rst.last()) {
				shortestLength = rst.getInt(1);
			}

			rst.beforeFirst(); // reset the result set

			// iter  over result set
			while (rst.next()) {
				int length = rst.getInt(1);
				String name = rst.getString(2);

				if (length == longestLength) {
					this.longestFirstNames.add(name);
				}
				if (length == shortestLength) {
					this.shortestFirstNames.add(name);
				}
			}

			// find the most common first names
			sql = "SELECT count(*), first_name FROM " + userTableName +
					" WHERE first_name IS NOT NULL " +
					" GROUP BY first_name " +
					" ORDER BY 1 DESC";
			rst = stmt.executeQuery(sql);
			int mostCommonNameCount = 0;
			if (rst.first()) {
				mostCommonNameCount = rst.getInt(1);
				this.mostCommonFirstNamesCount = mostCommonNameCount;
			}
			rst.beforeFirst();
			while (rst.next()) {
				int count = rst.getInt(1);
				String name = rst.getString(2);
				if (count == mostCommonNameCount) {
					this.mostCommonFirstNames.add(name);
				}
			}
			rst.close();
			stmt.close();


		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}

//		this.longestFirstNames.add("JohnJacobJingleheimerSchmidt");
//		this.shortestFirstNames.add("Al");
//		this.shortestFirstNames.add("Jo");
//		this.shortestFirstNames.add("Bo");
//		this.mostCommonFirstNames.add("John");
//		this.mostCommonFirstNames.add("Jane");
//		this.mostCommonFirstNamesCount = 10;
	}
	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have no friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void lonelyFriends() {
		// Find the following information from your database and store the information as shown
		final String COL_USER_ID = "user_id";
		final String COL_FIRST_NAME = "first_name";
		final String COL_LAST_NAME = "last_name";
		try (Statement stmt =
					 oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
			String sql = "SELECT U.user_id, U.first_name, U.last_name FROM " + userTableName + " U " +
					" WHERE NOT EXISTS(" +
					"    SELECT * FROM " + friendsTableName + " F " +
					"    WHERE U.user_id = F.user1_id OR U.user_id = F.user2_id" +
					")";
			ResultSet rst = stmt.executeQuery(sql);

			int count = 0;
			while (rst.next()) {
				String userIDStr = rst.getString(COL_USER_ID);
				long userID = Long.parseLong(userIDStr);
				String firstName = rst.getString(COL_FIRST_NAME);
				String lastName = rst.getString(COL_LAST_NAME);

				UserInfo user = new UserInfo(userID, firstName, lastName);
				this.lonelyFriends.add(user);
				count++;
			}

			this.countLonelyFriends = count;

			rst.close();
			stmt.close();

		} catch (SQLException e) {
			System.err.println(e.getMessage());
		}

//		this.lonelyFriends.add(new UserInfo(10L, "Billy", "SmellsFunny"));
//		this.lonelyFriends.add(new UserInfo(11L, "Jenny", "BadBreath"));
//		this.countLonelyFriends = 2;
	}
	 
	@Override
	// ***** Query 3 *****
	// Find the users who do not live in their hometowns
	// (I.e., current_city != hometown_city)
	//	
	public void liveAwayFromHome() throws SQLException {
		this.liveAwayFromHome.add(new UserInfo(11L, "Heather", "Movalot"));
		this.countLiveAwayFromHome = 1;
	}

	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) { 
		String photoId = "1234567";
		String albumId = "123456789";
		String albumName = "album1";
		String photoCaption = "caption1";
		String photoLink = "http://google.com";
		PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
		TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
		tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName1", "taggedUserLastName1"));
		tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName2", "taggedUserLastName2"));
		this.photosWithMostTags.add(tp);
	}

	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should return up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller user_id for the female
	// (iii) If there are still ties, choose the pair with the smaller user_id for the male
	//
	public void matchMaker(int n, int yearDiff)  { 
		Long girlUserId = 123L;
		String girlFirstName = "girlFirstName";
		String girlLastName = "girlLastName";
		int girlYear = 1988;
		Long boyUserId = 456L;
		String boyFirstName = "boyFirstName";
		String boyLastName = "boyLastName";
		int boyYear = 1986;
		MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
				girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
		String sharedPhotoId = "12345678";
		String sharedPhotoAlbumId = "123456789";
		String sharedPhotoAlbumName = "albumName";
		String sharedPhotoCaption = "caption";
		String sharedPhotoLink = "link";
		mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
				sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
		this.bestMatches.add(mp);
	}

	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n)  {
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

		p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
		p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
		p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
		this.suggestedFriendsPairs.add(p);
	}

	@Override
	// ***** Query 7 *****
	// 
	// Find the name of the state with the most events, as well as the number of 
	// events in that state.  If there is a tie, return the names of all of the (tied) states.
	//
	public void findEventStates()  {
		this.eventCount = 12;
		this.popularStateNames.add("Michigan");
		this.popularStateNames.add("California");
	}
	
	//@Override
	// ***** Query 8 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) {
		this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
		this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
	}

	@Override
	//	 ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
	// on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
	//  
	//
	public void findPotentialSiblings()  {
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
		this.siblings.add(s);
	}
	
}
