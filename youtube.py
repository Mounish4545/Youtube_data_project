from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st





def Api_connect():
    Api_Id="AIzaSyC-JTDlKubvoWCHkzPy2l0wvO36X54teeA"
    
    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    
    return youtube
youtube=Api_connect()

#channels information
def get_channel_info(channel_id):

    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                  Channel_Id=i['id'], 
                  Subscribers=i['statistics']['subscriberCount'],
                  Views=i["statistics"]["viewCount"],
                  Total_Videos=i["statistics"]["videoCount"],
                  Channel_Description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data      

channel_details=get_channel_info("UCCEQKydMgEFIXA4XDshj0CA")

channel_details

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                      part='contentDetails').execute()

    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists'][ 'uploads']

    next_page_token=None
    while True:

        response_1=youtube.playlistItems().list(
                                             part='snippet',
                                             playlistId=Playlist_Id,
                                             maxResults=50,
                                             pageToken=next_page_token).execute()
        for i in range(len(response_1['items'])):
            video_ids.append(response_1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response_1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids        

Video_Ids=get_videos_ids("UCCEQKydMgEFIXA4XDshj0CA")

Video_Ids

# video information

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id 
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'])
                      
            video_data.append(data)
    return video_data             

video_details=get_video_info(Video_Ids)

video_details

#get comments details
def get_comment_info(video_ids):
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


comment_details= get_comment_info(Video_Ids)

comment_details

# get playlist details
def get_playlist_details(channel_id):
    All_data=[]
    request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50

    )
    response=request.execute()

    for item in response['items']:
        data=dict(Playlist_Id=item['id'],
                  Title=item['snippet']['title'],
                  Channel_Id=item['snippet']['channelId'],
                  Channel_Name=item['snippet']['channelTitle'],
                  PublishedAt=item['snippet']['publishedAt'],
                  Video_Count=item['contentDetails']['itemCount'])
        All_data.append(data)
        
    return All_data      

playlist_details=get_playlist_details('UCCEQKydMgEFIXA4XDshj0CA')

playlist_details

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_project"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info( vi_ids)
    com_details=get_comment_info( vi_ids)

    coll=db["channel_details"]
    coll.insert_one({"channel_information": ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})

    return "upload completed"    

insert=channel_details('UCCEQKydMgEFIXA4XDshj0CA')


insert

   # channels table
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Mounish007@",
                            database="youtube_project",
                            port="5432")

    cursor=mydb.cursor()

    
    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(100) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(100))'''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("tables are insert")


    ch_list=[]
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df_1=pd.DataFrame(ch_list) 


    for index,row in df_1.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)

                                                values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
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
            print("channels value are insert")


import psycopg2

def insert_playlist_if_not_exists(playlist_id):
    conn = psycopg2.connect(database="youtube_project", user="postgres", password="Mounish007@", host="localhost", port="5432")
    cursor = conn.cursor()
    # Check if the playlist already exists
    cursor.execute("SELECT COUNT(*) FROM playlists WHERE playlist_id = %s", (playlist_id,))
    count = cursor.fetchone()[0]
    if count == 0:
        # If the playlist doesn't exist, insert it
        insert_query = "INSERT INTO playlists (playlist_id) VALUES (%s)"
        cursor.execute(insert_query, (playlist_id,))
        conn.commit()
    else:
        print("Playlist already exists in the database")
    cursor.close()
    conn.close()

# Call the function with the playlist ID
playlist_id = "PLs5oZZ3RfFiU6kOfUjOfMkNv46KK3x5uF"
insert_playlist_if_not_exists(playlist_id)


   # playlists table   # playlists table
def playlists_table():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Mounish007@",
                            database="youtube_project",
                            port="5432")

    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                            )'''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("tables are insert")


    play_list=[]    
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for play_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data["playlist_information"])):
            play_list.append(play_data["playlist_information"][i])
        
    df_2=pd.DataFrame(play_list)

    for index,row in df_2.iterrows():
        insert_query='''insert into playlists(Playlist_Id ,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count)

                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count']
                )
                
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("channels value are insert")

  # videos table
def videos_table():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Mounish007@",
                            database="youtube_project",
                            port="5432")

    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        channel_Id varchar(100),
                                                        Video_Id varchar(100) primary key,
                                                        Title varchar(200),
                                                        Tags text,
                                                        Thumbnail varchar(300),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(100),
                                                        Caption_Status varchar(100)
                                                            )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]    
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df_3=pd.DataFrame(vi_list)

    for index,row in df_3.iterrows():
                    insert_query='''insert into videos(Channel_Name,
                                                            Channel_Id,
                                                            Video_Id,
                                                            Title,
                                                            Thumbnail,
                                                            Description,
                                                            Published_Date,
                                                            Duration,
                                                            Views,
                                                            Likes,
                                                            Comments,
                                                            Favorite_Count,
                                                            Definition,
                                                            Caption_Status
                                                            )

                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    values=(row['Channel_Name'],
                            row['Channel_Id'],
                            row['Video_Id'],
                            row['Title'],
                            row['Thumbnail'],
                            row['Description'],
                            row['Published_Date'],
                            row['Duration'],
                            row['Views'],
                            row['Likes'],
                            row['Comments'],
                            row['Favorite_Count'],
                            row['Definition'],
                            row['Caption_Status']
                            )


                    cursor.execute(insert_query, values)
                    mydb.commit()

                                        

  # comments_table
def comments_table():
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="Mounish007@",
                                database="youtube_project",
                                port="5432")

        cursor=mydb.cursor()

        drop_query='''drop table if exists comments'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                Video_Id varchar(100),
                                                                Comment_Text text,
                                                                Comment_Author varchar(200),
                                                                Comment_Published timestamp
                                                                )'''



        cursor.execute(create_query)
        mydb.commit()

        comt_list=[]    
        db=client["youtube_project"]
        coll1=db["channel_details"]
        for comt_data in coll1.find({},{"_id":0,"comment_information":1}):
         for i in range(len(comt_data["comment_information"])):
                comt_list.append(comt_data["comment_information"][i])
        df_4=pd.DataFrame(comt_list)

        for index,row in df_4.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published
                                                )

                                                values(%s,%s,%s,%s,%s)'''
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']

                        )


                cursor.execute(insert_query, values)
                mydb.commit()


def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return ("Tables are created")
    

Tables=tables()

Tables

def show_channels_tables():
    ch_list=[]
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df_1=st.dataframe(ch_list) 

    return df_1


def show_playlists_tables():
    play_list=[]    
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for play_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data["playlist_information"])):
            play_list.append(play_data["playlist_information"][i])
        
    df_2=st.dataframe(play_list)

    return df_2

def show_videos_tables():
    vi_list=[]    
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df_3=st.dataframe(vi_list)

    return df_3

def show_comments_table():
    comt_list=[]    
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for comt_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comt_data["comment_information"])):
            comt_list.append(comt_data["comment_information"][i])
    df_4=st.dataframe(comt_list)

    return df_4

# streamlit

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Inegration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_project"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
      ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_ids:
       st.success("Channel Details of the given channel id already exists")

    else:
       insert=channel_details(channel_id) 
       st.success(insert)

if st.button("Migrate to Sql"):
   Tables=tables()
   st.success(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS",))

if show_table=="CHANNELS":
   show_channels_tables()

elif show_table=="PLAYLISTS":
   show_playlists_tables()

elif show_table=="VIDEOS":
   show_videos_tables()

elif show_table=="COMMENTS":
   show_comments_table()    


# sql connection

mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Mounish007@",
                            database="youtube_project",
                            port="5432")
cursor=mydb.cursor()


question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels",
                                              "2.channels have the most number of videos, and how many videos do they have",
                                              "3.What are the top 10 most viewed videos and their respective channels",
                                              "4.How many comments on each video, and what their corresponding video names",
                                              "5.videos have highest number of likes, and their corresponding channel names",
                                              "6.total number of likes and dislikeeach video their corresponding video names",
                                              "7.views for each channel, and what are their corresponding channel names",
                                              "8.channels that have published videos in the year 2022",
                                              "9.average duration of all videos in each channel",
                                              "10.videos with highest number of comments"))


if question=="1.What are the names of all the videos and their corresponding channels":
    query_1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query_1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df1)

elif question=="2.channels have the most number of videos, and how many videos do they have":
    query_2='''select channel_name as channelname,total_videos as no_videos from channels order by total_videos'''
    cursor.execute(query_2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)

elif question=="3.What are the top 10 most viewed videos and their respective channels":
    query_3='''select views as views,channel_name as channelname,title as videotitle from videos where views is not null order by views desc limit 10'''
    cursor.execute(query_3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)


elif question=="4.How many comments on each video, and what their corresponding video name":
    query_4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query_4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle",])
    st.write(df4)   

elif question=="videos have highest number of likes, and their corresponding channel names":
    query_5='''select title as videotitle,channel_name as channelname,likes as likecount from videos where likes is not null order by likes desc'''
    cursor.execute(query_5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)


elif question=="6.total number of likes and dislikeeach video their corresponding video names":
    query_6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query_6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)


elif question=="7.views for each channel, and what are their corresponding channel names":
    query_7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query_7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","total views"])
    st.write(df7) 

elif question=="8.channels that have published videos in the year 2022":
    query_8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos where extract(year from published_date)=2022'''
    cursor.execute(query_8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9.average duration of all videos in each channel":

    query_9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query_9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])


    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avg_duration=average_duration_str))
    df1=pd.DataFrame(T9)       
    st.write(df1)



elif question=="10.videos with highest number of comments":

    query_10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is  not null order by comments desc'''
    cursor.execute(query_10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)

