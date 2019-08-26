1. Purpose of the Database - Sparkify
 - To maintain a record of user activity on the music app for analytical purposes
 - The backend data provides an insight on the following :

     : number of users accessing the portal
     : to find the peak hours during which users are accessing the app
     : to find the most played song/artist info
     : to find the music favorites specific to a single user
     : provides geographical information where the app is most accessed : locations from which the most number of sessions are seen in a day/ or on an average

2. DataLake Design

The project creates a DataLake using Spark application and creates the following data schemas which is stored as parquet files on AWS S3 buckets :
    Dimensions --> 
     - User    : The User dimension schema has details/attributes specific to a user like - the userid, name, subscription details, session info , the time when the user accessed the app, for how long the app was accessed by the user and so on.     
     - Songs   : This schema has information related to songs like the song_name, title, artist, duration
     - Artists : This schema has information pertaining to the song artists (artist_id, artist_name, location etc)
     - Time : Dimension schema for storing the period values. The period info is necessary for analytical purposes (to find trends/patterns in songplay over a period of time )

   Facts : SONGPLAYS
          The fact schema stores the measures of user activity on the music app.
          This schema defines the user-song activity - like which user accessed which song/how many songs over a period of time.
          A song play can be defined as a combination of (user_id, song_id, session_id) attributes. This along with a sequence generated key will define the primary key in this table.

3.Example queries:

1. Number of users accessing the portal
     SELECT DISTINCT(user_id) from songplays

2. Most played song by a user
     SELECT user_id, (p.song_id),s.song_name, count(0) as cnt
      FROM songplays p, songs s
   WHERE p.song_id = s. song_id
    GROUP BY user_id,(p.song_id),s.song_name
    HAVING count(0)>1
    order by cnt desc
