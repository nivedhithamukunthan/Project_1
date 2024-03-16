import pymongo
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from streamlit_lottie import st_lottie


def Api():
    api_service_name = "youtube"
    api_version = "v3"
    api_key="AIzaSyCeR9eFcnviifGQ3OEGvYP08FHBQaDxwPc"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube
youtube=Api()


#Channel_datas
def channel_det(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    for i in response['items']:
        data=dict(Channel_name=i["snippet"]["title"],
                 Channel_id=i["id"],
                 Subscribers=i["statistics"]["subscriberCount"],
                 Channel_Desc=i["snippet"]["description"],
                 Views=i["statistics"]["viewCount"],
                 Tot_Videos=i["statistics"]["videoCount"],
                 Playlist=i["contentDetails"]["relatedPlaylists"]["uploads"]
                 )
        return data
    

#Getting video ids
def get_videos_of_channel(channel_id):
        video_ids=[]
        response = youtube.channels().list(
                part="contentDetails",
                #id="UCAHoSgSUADUYI2G4408DZYg"
                id=channel_id).execute()
        Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token=None
        while True:
            response1=youtube.playlistItems().list(
                part='snippet',
                playlistId=Playlist_Id,
                maxResults=50,
                pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')
    
            if next_page_token is None:
                break
        return video_ids


#Getting video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
           part="snippet,ContentDetails,statistics",
           id=video_id
        )
        response=request.execute()
        import pandas as pd
        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        for item in response['items']:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                     Channel_Id=item['snippet']['channelId'],
                      Video_Id=item['id'],
                      Title=item['snippet']['title'],
                      Tags=item['snippet'].get('tags',[]),
                      Thumbnail=item['snippet']['thumbnails']['default']['url'],
                      Description=item['snippet'].get('description'),
                      Published_date=item['snippet']['publishedAt'],
                      Duration=time_duration(item['contentDetails']['duration']),
                      Views=item['statistics'].get('viewCount'),
                      Comments=item['statistics'].get('commentCount'),
                      Likes=item['statistics'].get('likeCount'),
                      Favourite_Count=item['statistics']['favoriteCount'],
                      Definition=item['contentDetails']['definition'],
                      Caption_Status=item['contentDetails']['caption'],
                      
                      )
           
            video_data.append(data)
    return video_data



#Getting comments
def get_comments(video_ids):
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
                         Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         Comment_Publised=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data


#Mongo DB Connection
connection1=MongoClient("mongodb+srv://nivedhithamukund:gtB8eKWO6YCtjiiI@cluster0.mgyendn.mongodb.net/",connect=False)
db=connection1["Youtube_Project"]

def channel_information_for_mongo(channel_id):
    Channel_inf=channel_det(channel_id)
    Videos_Ids_inf=get_videos_of_channel(channel_id)
    Video_Details_inf=get_video_info(Videos_Ids_inf)
    Comments_inf=get_comments(Videos_Ids_inf)

    col2=db["Channels"]
    col2.insert_one({"Channels":Channel_inf,"Videos":Video_Details_inf,"Comments":Comments_inf})
    
    return "Upload Successful"


connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_Project")
mycursor=connection.cursor()



def channel_table(channel_name_single):
#Creating table for channel details in sql 

    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtube_project")
    #connection
    mycursor=connection.cursor()

    try:
        query='''create table Channels(
                                            Channel_name varchar(100),
                                            Channel_id varchar(100) primary key,
                                            Subscribers bigint,
                                            Channel_Desc varchar(600),
                                            Views bigint,
                                            Tot_Videos int,
                                            Playlist varchar(100))'''
        mycursor.execute(query)
        connection.commit()
    except:
        print("Table already exist")

    #Converting Mongo datas to DataFrame
    import pandas as pd
    db=connection1["Youtube_Project"]
    col2=db["Channels"]
    single_channel_list=[]
    for chan_data in col2.find({"Channels.Channel_name":channel_name_single},{"_id":0}):
        single_channel_list.append(chan_data["Channels"])
    df_single_channel_list=pd.DataFrame(single_channel_list)

    #Inserting DataFrame to SQl
    for i,r in df_single_channel_list.iterrows():
        query='insert into Channels(Channel_name,Channel_id,Subscribers,Channel_Desc,Views,Tot_Videos,Playlist) values(%s,%s,%s,%s,%s,%s,%s)'
        values=(r['Channel_name'],
                r['Channel_id'],
                r['Subscribers'],
                r['Channel_Desc'],
                r['Views'],
                r['Tot_Videos'],
                r['Playlist'])
        try:
            mycursor.execute(query,values)
            connection.commit()
        except:
            print("Data already exist")

#Creating table for Video details in sql

def video_table(channel_name_single):
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtube_project")
    #connection
    mycursor=connection.cursor()

    try:
        query='''create table Videos(
                                            Channel_name varchar(300),
                                            Channel_Id varchar(300),
                                            Video_Id varchar(300) primary key,
                                            Title varchar(300),
                                            Thumbnail varchar(300),
                                            Description text,
                                            Published_date varchar(300),
                                            Duration time,
                                            Views bigint,
                                            Comments int,
                                            Likes bigint,
                                            Favourite_Count int,
                                            Definition varchar(300),
                                            Caption_Status varchar(300)
                                            )'''
                                            
        mycursor.execute(query)
        connection.commit()
    except:
        print("Table already exists")

    #Mongo to DF
    import pandas as pd
    db=connection1["Youtube_Project"]
    col2=db["Channels"]
    single_video_list=[]
    for chan_data in col2.find({"Channels.Channel_name":channel_name_single},{"_id":0}):
        for i in range(len(chan_data["Videos"])):
            single_video_list.append(chan_data["Videos"][i])
    df_single_video_list=pd.DataFrame(single_video_list)


    #Inserting df to sql

    import pandas as pd

    for i,r in df_single_video_list.iterrows():
        video_query='''insert into Videos(
                                                Channel_name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Thumbnail,
                                                Description,
                                                Published_date,
                                                Duration,
                                                Views,
                                                Comments,
                                                Likes,
                                                Favourite_Count,
                                                Definition,
                                                Caption_Status
                                                ) 
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        video_values=(r['Channel_name'],
                r['Channel_Id'],
                r['Video_Id'],
                r['Title'],
                r['Thumbnail'],
                r['Description'], 
                r['Published_date'],
                r['Duration'],
                r['Views'],
                r['Comments'],
                r['Likes'],
                r['Favourite_Count'],
                r['Definition'],
                r['Caption_Status']
                )
        try:
            mycursor.execute(video_query,video_values)
            connection.commit()
        except Exception as es:
            print(es)
                                            
                                          
                                              
                
#Table for comments

#Table for comments
def comments_table(channel_name_single):
    connection=mysql.connector.connect(host="localhost",user="root",password="12345",database="youtube_project")
    mycursor=connection.cursor()
    
    try:
        query='''create table Comments(
                                            Comment_Id varchar(100) primary key,
                                            Video_id varchar(100),
                                            Comment_Text text,
                                            comment_Author varchar(150),
                                            Comment_Publised varchar(300))'''
        mycursor.execute(query)
        connection.commit()
    except:
        print("Table already exist")

    #Mongo to df
    import pandas as pd
    db=connection1["Youtube_Project"]
    col2=db["Channels"]
    single_comments_list=[]
    for chan_data in col2.find({"Channels.Channel_name":channel_name_single},{"_id":0}):
        for i in range(len(chan_data["Comments"])):
            single_comments_list.append(chan_data["Comments"][i])
    df_single_comment_list=pd.DataFrame(single_comments_list)


    #inserting df to sql table
    for i,r in df_single_comment_list.iterrows():
        query='insert into Comments(Comment_Id,Video_id,Comment_Text,comment_Author,Comment_Publised) values(%s,%s,%s,%s,%s)'
        values=(r['Comment_Id'],
                r['Video_id'],
                r['Comment_Text'],
                r['comment_Author'],
                r['Comment_Publised'])
        try:
            mycursor.execute(query,values)
            connection.commit()
        except Exception as es:
            print(es)
        


def table_for_channel(channel_names):
    channel_table(channel_names)
    x=f"Table for {channel_names} channel created successfully"
    return x
def table_for_video(channel_names):
    video_table(channel_names)
    x=f"Table for {channel_names} channel's video created successfully"
    return x
def table_for_comment(channel_names):
    comments_table(channel_names)
    x=f"Table for {channel_names} channel's comment created successfully"
    return x

   


def show_channels_table():
    channel_list=[]
    db=connection1["Youtube_Project"]
    col2=db["Channels"]
    for channel_data in col2.find({},{"_id":0,"Channels":1}):
        channel_list.append(channel_data["Channels"])
    df=st.dataframe(channel_list)
    return df
    


def show_video_table():
    vi_list=[]
    db=connection1["Youtube_Project"]
    col2=db["Channels"]
    for vi_data in col2.find({},{"_id":0,"Videos":1}):
        for i in range(len(vi_data["Videos"])):
            vi_list.append(vi_data["Videos"][i])
    df1=st.dataframe(vi_list)
    return df1

def show_comment_table():
    comment_list=[]
    db=connection1["Youtube_Project"]
    col2=db["Channels"]
    for com_data in col2.find({},{"_id":0,"Comments":1}):
        for i in range(len(com_data["Comments"])):
            comment_list.append(com_data["Comments"][i])
    df2=st.dataframe(comment_list)
    return df2


#StreamLit
with st.sidebar:
    from PIL import Image
    st.title(":red[Youtube Project]")
    image=Image.open(r"C:\Users\mukun\Desktop\logo.png")
    st.image(image)
tab_title=[":blue[Migration of Datas]",
           ":blue[Tables]",
           ":blue[Questions]"]
tabs=st.tabs(tab_title)

with tabs[0]:
    channel_id_streamlit=st.text_input("Enter the Channel ID")
    image2=Image.open(r"C:\Users\mukun\Desktop\mongo_logo.png")
    st.image(image2,width=50)
    if st.button("Store Datas to Mongo"):
        ch_ids=[]
        db=connection1["Youtube_Project"]
        col2=db["Channels"]
        for ch_data in col2.find({},{"_id":0,"Channels":1}):
            ch_ids.append(ch_data["Channels"]["Channel_id"])
        if channel_id_streamlit in ch_ids:
            st.success(":red[Channel detail already exist!!!!]")
        else:
            insert=channel_information_for_mongo(channel_id_streamlit)
            st.success(insert)
    
    channel_names=[]
    db=connection1["Youtube_Project"]
    col2=db["Channels"]
    for channel_names_data in col2.find({},{"_id":0,"Channels":1}):
        channel_names.append(channel_names_data["Channels"]["Channel_name"])

    single_channel=st.selectbox("Show channel names",channel_names)

    image3=Image.open(r"C:\Users\mukun\Desktop\sql_logo.png")
    st.image(image3,width=50)
    if st.button("Migrate to sql"):
            Table1=table_for_channel(single_channel)
            st.success(Table1)

            Table2=table_for_video(single_channel)
            st.success(Table2)
    
            Table3=table_for_comment(single_channel)
            st.success(Table3)
with tabs[1]:
    show_table=st.radio("Select the tables",("CHANNELS","VIDEOS","COMMENTS"))
    if show_table=="CHANNELS":
        show_channels_table()
    if show_table=="VIDEOS":
        show_video_table()
    if show_table=="COMMENTS":
        show_comment_table()
    


#Sql connection for 10 questions:

with tabs[2]:

    question=st.selectbox("Your Questions",("Click to select your Questions",
                                            "1.What are the names of all the videos and their corresponding channels?",
                                            "2.Which channels have the most number of videos, and how many videos do they have?",
                                            "3.What are the top 10 most viewed videos and their respective channels?",
                                            "4.How many comments were made on each video, and what are their corresponding video names?",
                                            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                            "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                            "8.What are the names of all the channels that have published videos in the year 2022?",
                                            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                            "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
                                            ))

    if question=="1.What are the names of all the videos and their corresponding channels?":
        query1="SELECT Title,Channel_name FROM youtube_project.videos"
        mycursor.execute(query1)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Video Title","Channel Name"])
        st.write(df_sql)


    if question=="2.Which channels have the most number of videos, and how many videos do they have?":
        query2='''SELECT Tot_Videos,Channel_name FROM youtube_project.channels order by Tot_Videos desc
                    LIMIT 1'''
        mycursor.execute(query2)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Video Count","Channel Name"])
        st.write(df_sql)

    if question=="3.What are the top 10 most viewed videos and their respective channels?":
        query3='''SELECT Views,Channel_name FROM youtube_project.videos 
        order by Views desc
        LIMIT 10;'''
        mycursor.execute(query3)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Maximum Views","Channel Name"])
        st.write(df_sql)
    if question=="4.How many comments were made on each video, and what are their corresponding video names?":
        query4='''SELECT Comments,Title FROM youtube_project.videos'''
        mycursor.execute(query4)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Comments_number","Video Name"])
        st.write(df_sql)
    if question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5='''SELECT Channel_name,Likes FROM youtube_project.videos order by Likes desc
                    LIMIT 1'''
        mycursor.execute(query5)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Channel_Names","Likes"])
        st.write(df_sql)
    if question=="6.What is the total number of likes for each video, and what are their corresponding video names?":
        query6='''SELECT title,Likes FROM youtube_project.videos'''
        mycursor.execute(query6)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Video_Names","Likes"])
        st.write(df_sql)
    if question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query7='''SELECT Channel_name,Views FROM youtube_project.channels'''
        mycursor.execute(query7)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Channel_Names","Views"])
        st.write(df_sql)
    if question=="8.What are the names of all the channels that have published videos in the year 2022?":
        query8="SELECT CONVERT(Published_date, DATE) AS Date,Channel_name from youtube_project.videos where Published_date like '2022%'"
        mycursor.execute(query8)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Published Date starting from 2022","Channel Names"])
        st.write(df_sql)
    if question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9='''SELECT Channel_name, AVG(Duration) from youtube_project.videos group by Channel_name'''
        mycursor.execute(query9)
        res=mycursor.fetchall()
        df_sql=pd.DataFrame(res,columns=["Channel Names","Average Duration"])
        st.write(df_sql)
    
    if question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
            query10='''SELECT Channel_name,Comments FROM youtube_project.videos order by Comments desc
                        LIMIT 1'''
            
            mycursor.execute(query10)
            res=mycursor.fetchall()
            df_sql=pd.DataFrame(res,columns=["Channel_Names","Highest Comment"])
            st.write(df_sql)
    


        

