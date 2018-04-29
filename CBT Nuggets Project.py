
# coding: utf-8

# # CBT Nuggets Project
# This Jupyter Notebook is where work is done to import the Slack data export and find the answers to the given questions: 
# 
# - What channel had the most posts per week/month? (filter out non manual entries, do both peak week/month from total dataset and average for each channel)
# - What's the average number of posts per user in a week/month? 
# - Which user had the highest number of posts in a single week (and how many was it)?
# - Which user had the highest number of posts in a single month (and how many was it)?
# - Which user had the highest number of posts the most recent full month in the 'general-discussion' channel (and how many was it)?

# In[1]:


# import necessary libraries
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
from os import listdir
from os.path import isfile, isdir, join


# In[2]:


# the directory is the unzipped slack data export
directory = 'CBT Learners Slack export Mar 21 2018/'
# the subdirectories are all the folders referencing each channel in the Slack group
subdirectories = [folder for folder in listdir(directory) if isdir(join(directory, folder))]


# In[3]:


# we will pull in all the data from each channel and concatenate it into one dataframe. We will also add a 'channel'
# column to preserve where each message came from. 

# we will first initialize df as an empty dataframe
df = pd.DataFrame(columns = ['channel'])
# next we will do a for loop to import all the files from each channel and append to df
for subdir in subdirectories:
    mypath = directory + subdir
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    value = pd.concat([pd.read_json(mypath + '/' + file) for file in onlyfiles], ignore_index=True)
    value['channel'] = subdir
    df = df.append(value)
    


# In[4]:


# to answer the questions, we don't need the entire dataset. We will keep only the relevant columns
df = df[['channel','subtype','ts','user']]
df.isnull().sum()


# In[5]:


# we see that subtype and user have some null values. We need to fill these spaces

# for subtype, the null values are messages that were manually entered by the user. these are the only rows we want
df = df[df['subtype'].isnull()]
# we will drop 'subtype' as it is irrelevant now
df = df.drop('subtype', axis=1)

# check to make sure the dataset is now clean
df.isnull().sum() 


# In[6]:


## Question 1: What channel had the most posts per week/month? (filter out non manual entries, do both peak week/month from total dataset and average for each channel)
# to answer the first question, we will add columns for 'week_number' and 'month'

# convert ts from seconds since epoch to a pandas datetime format
df['ts'] = pd.to_datetime(df['ts'],unit='s')
# week_number is the week number for a given year. This prevents week one from multiple years being summed. It is 
# in the format 'YYYY-w##' 
df['week_number'] = df['ts'].dt.strftime('%Y-w%W')
# month is the month number for a given year. This prevents month one from multiple years being summed. It is 
# in the format 'YYYY-m##' 
df['month'] = df['ts'].dt.strftime('%Y-m%m')

# calculate and print results
peak_week = df.groupby(['channel','week_number'])['ts'].count().idxmax()
print('the channel with the mosts weekly posts was {} on week {}'.format(peak_week[0],peak_week[1]))
peak_month = df.groupby(['channel','month'])['ts'].count().idxmax()
print('the channel with the mosts monthly posts was {} on month {}'.format(peak_month[0],peak_month[1]))


# In[7]:


# plt.figure(figsize=(20,5))
# sns.countplot(df['week_number'].sort_values())
# plt.title('Histogram of posts per week')
# plt.xticks(rotation=90)
# plt.show()


# In[8]:


# plt.figure(figsize=(20,5))
# sns.countplot(df['month'].sort_values())
# plt.title('Histogram of posts per month')
# plt.xticks(rotation=90)
# plt.show()


# In[9]:


# question 1 part 2 is calculating the average posts/(week and month) for each channel. We will calculate that here

# calculate the total posts for each channel
channels_total_posts = df.groupby(['channel'])['ts'].count()
# calculate the start date of each channel
channels_start = df.groupby(['channel'])['ts'].min()
# pull the last timestamp, this is the "present"
channel_end = df['ts'].max()
# calculate each channel's age in weeks
channels_age_weeks = (channel_end - channels_start)/ np.timedelta64(1, 'W')
# calculate each channel's age in months
channels_age_months = (channel_end - channels_start)/ np.timedelta64(1, 'M')

# calculate and print the average posts per week for all channels
print('the average posts per week for each {}'.format(channels_total_posts/channels_age_weeks))
# calculate and print the average posts per month for all channels
print('\nthe average posts per month for each {}'.format(channels_total_posts/channels_age_months))


# In[10]:


# What's the average number of posts per user in a week/month?
# to answer the second question, we need to first measure the age of a user's account. Then divide the total
# number of posts by that user by its age (in weeks or months as unit). 

# calculate the total posts for each user
users_total_posts = df.groupby(['user'])['ts'].count()
# calculate the start date of each user
users_start = df.groupby(['user'])['ts'].min()
# pull the last timestamp, this is the "present"
user_end = df['ts'].max()
# calculate each user's age in weeks
users_age_weeks = (user_end - users_start)/ np.timedelta64(1, 'W')
# calculate each user's age in months
users_age_months = (user_end - users_start)/ np.timedelta64(1, 'M')

# calculate and print the average posts per week for all users
print('the average posts per week for each {}'.format(users_total_posts/users_age_weeks))
# calculate and print the average posts per month for all users
print('\nthe average posts per month for each {}'.format(users_total_posts/users_age_months))


# In[11]:


## Question 3: What user had the most posts total in a week?

# we will calculate which user had the most weekly posts
users_weekly_posts_maxidx = df.groupby(['user','week_number'])['ts'].count().idxmax()
users_weekly_posts_max = df.groupby(['user','week_number'])['ts'].count().max()
print('user {} had the most weekly posts with {} posts'.format(users_weekly_posts_maxidx[0], users_weekly_posts_max))


# In[12]:


## Question 4: What user had the most posts total in a month?

# we will calculate which user had the most monthly posts
users_monthly_posts_maxidx = df.groupby(['user','month'])['ts'].count().idxmax()
users_monthly_posts_max = df.groupby(['user','month'])['ts'].count().max()
print('user {} had the most monthly posts with {} posts'.format(users_monthly_posts_maxidx[0], users_monthly_posts_max))


# In[13]:


## Question 5: Which user had the highest number of posts the most recent full month in the 'general-discussion' 
# channel (and how many was it)?

# we queried earlier that the last timestamp is on 3/21/2018. That means we want to investigate the month of 
# February for this problem
gen_disc_max_user = df[(df['channel']=='general-discussion') & (df['month']=='2018-m02')].groupby(['user'])['ts'].count().idxmax()
gen_disc_max_posts = df[(df['channel']=='general-discussion') & (df['month']=='2018-m02')].groupby(['user'])['ts'].count().max()
print('user {} had the most posts in Feb-2018 with {} posts'.format(gen_disc_max_user, gen_disc_max_posts))

