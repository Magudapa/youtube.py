from googleapiclient.discovery import  build
import pymongo 
import psycopg2
import pandas as pd
import streamlit as st

# API CONNECTION 
def Api_connect():
    Api_Id="AIzaSyBwoEioGpdkssG3m6y8KM0AcTpDB9Yorjc"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube
youtube=Api_connect()

# CHANNEL DETAILS  FUNCTION
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    data = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Subscribers': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Id': response['items'][0]['id'],
        'Views': response['items'][0]['statistics']['viewCount'],
        'Total_Videos': response['items'][0]['statistics']['videoCount'],
        'Playlist_Id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'Channel_Description': response['items'][0]['snippet']['description']
        }
    return data
        

# VIDEO IDS FUNCTION
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part='contentDetails').execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
            part = 'snippet',
            playlistId = playlist_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    return video_ids

# VIDEO DETAILS FUNCTION
def get_video_details(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response['items']:
            data = {
                'Channel_Name': item['snippet']['channelTitle'],
                'Channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Title': item['snippet']['title'],
                'Tags': item['snippet'].get('tags'),
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet']['description'],
                'Published_Date': item['snippet']['publishedAt'],
                'Duration': item['contentDetails']['duration'],
                'Views': item['statistics'].get('viewCount'),
                'Likes': item['statistics'].get('likeCount'),
                'Comments': item['statistics'].get('commentCount'),
                'Favorite_Count': item['statistics'].get('favoriteCount'),
                'Definition': item['contentDetails']['definition'],
                'Caption_Status': item['contentDetails']['caption']
            }
            video_data.append(data)
    return video_data

# COMMENT DETAILS
def get_comment_details(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

# PLAYLIST DETAILS
def get_playlist_details(channel_id):
    playlist_data = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            data = {
                'Playlist_Id': item['id'],
                'Title': item['snippet']['title'],
                'channel_Id': item['snippet']['channelId'],
                'Channel_Name': item['snippet']['channelTitle'],
                'PublishedAt': item['snippet']['publishedAt'],
                'Video_Count': item['contentDetails']['itemCount']
            }
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return playlist_data
    
# MONGODB CONNECTION
client = pymongo.MongoClient("mongodb+srv://magudapathi:magu1234@magudapa.tyhoqeh.mongodb.net/")
db = client["Youtube_data"]

# CHANNEL INFORMATION FUNCTION
def channel_informations(channel_id):
    channel_info = get_channel_details(channel_id)
    playlist_info = get_playlist_details(channel_id)
    video_ids = get_videos_ids(channel_id)
    videos_info =get_video_details(video_ids)
    comment_info = get_comment_details(video_ids)
    

    col1 = db["channel_informations"]
    col1.insert_one({"channel_information":channel_info,"playlist_information":playlist_info,"video_information":videos_info,
                     "comment_information":comment_info})
    
    return "upload completed "

# CHANNELS TABLE FUNCTION
def channels_table(channel_name_1):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="magu",
                            database="YT_data",
                            port= "5432")
    cursor = mydb.cursor()


    create_query='''CREATE TABLE IF NOT EXISTS Channels(Channel_Name VARCHAR(100),
                                                        Channel_Id VARCHAR(80) primary key, 
                                                        Subscribers BIGINT, 
                                                        Views BIGINT,
                                                        Total_videos INT,
                                                        Channel_Description  TEXT,
                                                        Playlist_Id VARCHAR(80))'''
    cursor.execute(create_query)
    mydb.commit()


    query_1= "SELECT * FROM channels"
    cursor.execute(query_1)
    table= cursor.fetchall()
    mydb.commit()

    chan_list= []
    chan_list2= []
    df_all_channels= pd.DataFrame(table)

    chan_list.append(df_all_channels[0])
    for i in chan_list[0]:
        chan_list2.append(i)
    

    if channel_name_1 in chan_list2:
        news= f"Your Provided Channel {channel_name_1} is Already exists"        
        return news
    
    else:

        single_channel_details= []
        col1=db["channel_informatons"]
        for ch_data in col1.find({"channel_information.Channel_Name":channel_name_1},{"_id":0}):
            single_channel_details.append(ch_data["channel_information"])

        df_single_channel= pd.DataFrame(single_channel_details)
    
    for index,row in df_single_channel.iterrows():
        insert_query = '''insert into channels(Channel_Name ,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        values =(row['Channel_Name'],  # sclicing the dataframe
                 row['Channel_Id'],
                 row['Subscribers'],
                 row['Views'],
                 row['Total_Videos'],
                 row['Channel_Description'],
                 row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()
                
        except:
            print("Channels values are already inserted")
    

# VIDEOS TABLE FUNCTION
def videos_table(channel_name_1):
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="YT_data",
                        port= "5432")
    cursor = mydb.cursor()


    create_query='''CREATE TABLE IF NOT EXISTS videos(Channel_Name varchar(100),
        Channel_Id varchar(100),
        Video_Id varchar(100)primary key,
        Title varchar(100),
        Tags text,
        Thumbnail varchar(225),
        Description text,
        Published_Date timestamp,
        Duration interval,
        Views BIGINT,
        Likes BIGINT,
        Comments BIGINT,
        Favorite_Count BIGINT,
        Definition varchar(30),
        Caption_Status varchar(60))'''
    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_informations"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_1},{"_id":0}):
        single_channel_details.append(ch_data["video_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index, row in df_single_channel.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status) 
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print("An error occurred:", e)
            mydb.rollback() 


# COMMENTS TABLE FUNCTION
def comments_table(channel_name_1):

    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="magu",
                        database="YT_data",
                        port= "5432")
    cursor = mydb.cursor()


    create_query='''CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(100) primary key,
                                                    Video_Id varchar(100),
                                                    Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_Published timestamp)''' 
    cursor.execute(create_query)
    mydb.commit()


    single_channel_details= []
    col1=db["channel_informations"]
    for ch_data in col1.find({"channel_information.Channel_Name":channel_name_1},{"_id":0}):
        single_channel_details.append(ch_data["comment_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index, row in df_single_channel.iterrows():
        insert_query = '''insert into comments(comment_id,
                                                video_id,
                                                comment_text,
                                                comment_author,
                                                comment_published)
                        VALUES(%s, %s, %s, %s, %s)'''
        values = (row['Comment_Id'],  
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
        
            
        cursor.execute(insert_query,values)
        mydb.commit()


            
# TABLES FUNCTION            
def tables(channel_name):

    news= channels_table(channel_name)
    if news:
        st.write(news)
    else:
        videos_table(channel_name)
        comments_table(channel_name)

    return "Tables Created Successfully"

# SHOW CHANNELS TABLES FUNCTION
def show_channels_tables():
    ch_list = []
    db = client["Youtube_data"]
    col1 = db["channel_informations"]
    for ch_data in col1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list) 
    return df

# SHOW VIDEO TABLES FUNCTION
def show_videos_tables():
    vid_list = []
    db = client["Youtube_data"]
    col1 = db["channel_informations"]
    for vid_data in col1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vid_data["video_information"])):
            vid_list.append(vid_data["video_information"][i])
    df1=st.dataframe(vid_list)
    return df1

# SHOW COMMENTS TABLES FUNCTION
def  show_comments_tables():
    com_list = []
    db = client["Youtube_data"]
    col1 = db["channel_informations"]
    for com_data in col1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df2=st.dataframe(com_list)
    return df2

# STREAMLIT OUTPUT FUNCTION
channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    col1=db["channel_informations"]
    for ch_data in col1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_informations(channel_id)
        st.success(insert)
        
all_channels= []
col1=db["channel_informations"]
for ch_data in col1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
unique_channel= st.selectbox("Select the Channel",all_channels)

if st.button("Migrate to Sql"):
    Tables=tables(unique_channel)
    st.success(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_tables()

elif show_table=="VIDEOS":
    show_videos_tables()

elif show_table=="COMMENTS":
    show_comments_tables()


# MAIN FUNCTION 
mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="magu",
                    database="YT_data",
                    port= "5432")
cursor = mydb.cursor()


question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

# youtube.py  function calls based on question selected by user
if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
