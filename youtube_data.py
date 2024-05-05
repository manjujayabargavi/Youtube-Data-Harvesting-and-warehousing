from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
#from streamlit_option_menu import option_menu
import time
st.set_page_config(page_title="Youtube Data Analysis", page_icon=":anchor:", layout="wide", menu_items=None)

st.title(":red[Youtube Data] :rainbow[Harvesting Warehousing] ")

channel_id=st.text_input("Enter the Channel ID")

api_id = "AIzaSyD7RBUiV-4YGOwljRS_QDKg2H4QwViUlxI"

youtube = build("youtube","v3",developerKey=api_id)

st.subheader(":orange[Uploading to MongoDB Database]")

#get channel information
def get_channel_info(youtube,channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for i in response['items']:
        data = dict(channel_name = i["snippet"]["title"],
                channel_id=i['id'],
                subs=i['statistics']['subscriberCount'],
                views=i['statistics']['viewCount'],
                Totalvideos=i['statistics']['videoCount'],
                channel_desc=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data

#get playlist information
def get_playlist(youtube,channel_id):
    next_page_token=None
    playlist=[]
    while True:
        request = youtube.playlists().list(
             part='snippet,contentDetails',
             channelId=channel_id,
             maxResults=50,
             pageToken=next_page_token,
             
            )
        response = request.execute()
        for item in response['items']:
            data3=dict(playlist_id= item['id'],
                       title= item['snippet']['title'],
                       channel_id= item['snippet']['channelId'],
                       channel_name= item['snippet']['channelTitle'],
                       publishedat= item['snippet']['publishedAt'],
                       videocount=item['contentDetails']['itemCount']
                      )
            playlist.append(data3)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist

#get video ids 
def get_video_ids(youtube,channel_id):
    video_ids=[]
    video = youtube.channels().list(
            part='contentDetails',
            id=channel_id).execute()
    playlist_id = video['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    
    while True:
        video1= youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(video1['items'])):
            video_ids.append(video1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=video1.get('nextPageToken')
        if next_page_token is None:
             break
        return video_ids
    

#get video information
def get_video_info(youtube,videoids):
    video_data=[]
    for video_id in videoids:
        request = youtube.videos().list(
            part= 'snippet,contentDetails,statistics',
            id=video_id
        )
        response= request.execute()  


        for item in response['items']:

            #Published_date = item["snippet"]["publishedAt"]
            #parsed_dates = datetime.strptime(Published_date,'%Y-%m-%dT%H:%M:%SZ')
            #format_date = parsed_dates.strftime('%Y-%m-%d')

            data1=dict(channel_name=item['snippet']['channelTitle'],
                      channel_id=item['snippet']['channelId'],
                      video_id=item['id'],
                      video_title=item['snippet']['title'],
                      Describtion=item['snippet'].get('description'),
                      #Published_date=format_date,
                      Published_date = item["snippet"]["publishedAt"],
                      Duration=item['contentDetails']['duration'],
                      Views=item['statistics'].get('viewCount'),
                      Like_count=item['statistics'].get('likeCount'),
                      Comment_Count=item['statistics'].get('commentCount')
                      
            )
            video_data.append(data1)
    return video_data


#get comment information
def get_comment_info(youtube,videoids):
    comment_data=[]

    try:

        for video_id in videoids:
            request= youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data2=dict(comment_id=item['snippet']['topLevelComment']['id'],
                            video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                        )
                comment_data.append(data2)
    except:
        pass
    
    return comment_data


#importing the data to mongoDB

submit1=st.button("Data Harvest and Migration")

client= pymongo.MongoClient("mongodb://localhost:27017/")

db= client['capstone_project']
coll1 = db['channel_details']

if submit1:

    if channel_id:

        ch_details= get_channel_info(youtube,channel_id)
        playlist_details= get_playlist(youtube,channel_id)
        vi_ids= get_video_ids(youtube,channel_id)
        video_details= get_video_info(youtube,vi_ids)
        comment_details= get_comment_info(youtube,vi_ids)

        with st.spinner('Please wait '):
            time.sleep(5)

        ch_ids=[]

        for ch_data in coll1.find({},{"_id":0,"channel_info":1}):

            ch_ids.append(ch_data["channel_info"]["channel_id"])

        if channel_id in ch_ids:

            st.success("Channel Details already exists")
        else:
            coll1.insert_one({"channel_info":ch_details,
                        "playlist_info":playlist_details,
                        'video_info':video_details,
                        'comment_info':comment_details})
        
        with st.spinner('Please wait '):
            time.sleep(5)
            st.success('Data collected and Uploaded to MongoDB')
            st.balloons()


#table creations
ch_names=[]
db=client["capstone_project"]
coll1=db["channel_details"]
for i in coll1.find({},{"_id":0,"channel_info":1}):
    ch_names.append(i["channel_info"]["channel_name"])  

st.subheader(":orange[Inserting Data into SQL for further Data Analysis]")

select_channel= st.selectbox("Select the Channel",options=ch_names)

#channel tables
def channel_table(user):       
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root",
                        database="youtube",
                        port="5432")
    mycursor=mydb.cursor()

    query= '''CREATE TABLE if not exists channels(channel_name varchar(100),channel_id varchar(100) primary key,subs bigint,views bigint,Totalvideos int,channel_desc text,
                                        playlist_id varchar(100))'''
    mycursor.execute(query)
    mydb.commit()

    db=client["capstone_project"]
    coll1=db["channel_details"]
    
    ch=coll1.find_one({"channel_info.channel_name":user},{"_id":0})

    tu_ch=tuple(ch["channel_info"].values())
    
    insert_query= '''insert into channels(channel_name, channel_id, subs, views, Totalvideos, channel_desc, playlist_id)
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
    
    try:
    
        mycursor.execute(insert_query,tu_ch)
        mydb.commit()

    except:

        exists=f"{user} already exists!!! Try entering new channel"

        return exists

#video table
def video_table(user):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="root",
                        database="youtube",
                        port="5432")
        mycursor=mydb.cursor()

        create_query= '''CREATE TABLE if not exists video(channel_name varchar(100),channel_id varchar(100),video_id varchar(100) primary key,video_title varchar(100),
        Describtion text,Published_date timestamp,Duration interval,Views bigint,Like_count bigint,Comment_Count bigint)'''
        
        mycursor.execute(create_query)
        mydb.commit()
        
        vi=coll1.find_one({"channel_info.channel_name":user},{"_id":0})

        query = '''insert into video(channel_name,channel_id,video_id,video_title,Describtion,Published_date,Duration,Views,Like_count,Comment_Count)

                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        for i in vi['video_info']:
                value=tuple(i.values())
                mycursor.execute(query,value)
                mydb.commit()

#comment table creation
def comment_table(user):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="root",
        database="youtube",
        port="5432"
        )
    mycursor=mydb.cursor()

    create_query='''create table if not exists comment(comment_id varchar(100) primary key,video_id varchar(100),comment_text text,comment_author varchar(100),
                                            comment_published timestamp)'''
    
    mycursor.execute(create_query)
    mydb.commit()

    cm = coll1.find_one({"channel_info.channel_name":user},{"_id":0})

    insert_query='''insert into comment(comment_id,video_id,comment_text,comment_author,comment_published)
                                            values(%s,%s,%s,%s,%s)'''
    
    for i in cm["comment_info"]:
        value=tuple(i.values())
        mycursor.execute(insert_query,value)
        mydb.commit()

submit = st.button("Upload data into MySQL")

def tables(name):

    exists=channel_table(name)

    if exists:
        return exists
    else:
        video_table(name)
        comment_table(name)

        return "Tables created Successfully!!! You can start your Analysis"

if submit:

    Tables=tables(select_channel)
    with st.spinner('Please wait '):
        time.sleep(5)
        st.success(Tables)



#SQL CONNECTIONS
mydb=psycopg2.connect(
host="localhost",
user="postgres",
password="root",
database='youtube',
port='5432'
)

mycursor = mydb.cursor()

questions= st.selectbox("Select Your Questions",
                        ['Click the question that you would like to analyse',
                        "1. Name of all the videos and their corresponding channel",
                        "2. Channels have heighest number of videos",
                        "3. The top 10 most viewed videos and their respective channels",
                        "4. Comments made on each videos and their corresponding video names",
                        "5. Videos have the highest number of likes and their corresponding channel names",
                        "6. Likes of all the videos and their corresponding video names",
                        "7. Views of each channel and their corresponding channel names",
                        "8. Video published in the year of 2022",
                        "9. Average duration of all videos in each channel",
                        "10. Videos with heighests number of comments"])
                                                  


if questions=="1. Name of all the videos and their corresponding channel":

    query1='''select video_title,channel_name from video'''

    mycursor.execute(query1)
    mydb.commit()

    f1=mycursor.fetchall()
    df_f1=pd.DataFrame(f1,columns=['video title','channel name'])
    st.write(df_f1)


elif questions=="2. Channels have heighest number of videos":

    query2='''select channel_name,totalvideos from channels
    order by totalvideos desc'''

    mycursor.execute(query2)
    mydb.commit()

    f2=mycursor.fetchall()
    df_f2=pd.DataFrame(f2,columns=['channel name','totalvideos'])
    st.write(df_f2)

elif questions=="3. The top 10 most viewed videos and their respective channels":

    query3='''select channel_name,video_title,views from video
                order by views desc
                limit 10'''

    mycursor.execute(query3)
    mydb.commit()

    f3=mycursor.fetchall()
    df_f3=pd.DataFrame(f3,columns=['channel name','video_title','views'])
    st.write(df_f3)

elif questions=="4. Comments made on each videos and their corresponding video names":

    query4='''select comment_count,video_title from video where comment_count is not null'''

    mycursor.execute(query4)
    mydb.commit()

    f4=mycursor.fetchall()
    df_f4=pd.DataFrame(f4,columns=['comment_count','video_title'])
    st.write(df_f4)


elif questions=="5. Videos have the highest number of likes and their corresponding channel names":

    query5='''select channel_name,video_title,comment_count from video
    order by comment_count desc
    limit 10'''

    mycursor.execute(query5)
    mydb.commit()

    f5=mycursor.fetchall()
    df_f5=pd.DataFrame(f5,columns=['channel_name','video_title','max_like'])
    st.write(df_f5)

elif questions=="6. Likes of all the videos and their corresponding video names":

    query6='''select channel_name,sum(like_count) as total_likes from video
    group by channel_name
    order by total_likes desc'''

    mycursor.execute(query6)
    mydb.commit()

    f6=mycursor.fetchall()
    df_f6=pd.DataFrame(f6,columns=['channel_name','total_likes'])
    st.write(df_f6)

elif questions=="7. Views of each channel and their corresponding channel names":

    query7='''select channel_name,views from channels 
    order by views desc'''
    mycursor.execute(query7)
    mydb.commit()

    f7=mycursor.fetchall()
    df_f7=pd.DataFrame(f7,columns=['channel_name','views'])
    st.write(df_f7)

elif questions=="8. Video published in the year of 2022":

    query8='''SELECT channel_name,video_title,published_date FROM video
    where extract(year from published_date)=2022'''
    mycursor.execute(query8)
    mydb.commit()

    f8=mycursor.fetchall()
    df_f8=pd.DataFrame(f8,columns=['channel_name','video_title','published_date'])
    st.write(df_f8)


elif questions=="9. Average duration of all videos in each channel":

    query9='''select channel_name,avg(duration) as average_duration from video
    group by channel_name'''
    mycursor.execute(query9)
    mydb.commit()

    f9=mycursor.fetchall()
    df_f9=pd.DataFrame(f9,columns=['channel_name','average_duration'])
    st.write(df_f9)


elif questions=="10. Videos with heighests number of comments":

    query10='''select channel_name,video_title,sum(comment_count) as comment_count from video
    group by channel_name,video_title
    order by comment_count desc'''
    mycursor.execute(query10)
    mydb.commit()

    f10=mycursor.fetchall()
    df_f10=pd.DataFrame(f10,columns=['channel_name','video_title','comment_count'])
    st.write(df_f10)







