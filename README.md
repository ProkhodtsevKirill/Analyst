Hi! In this case I want to present a simple analyst dashboard for some app, ETL pipeline, and reporting task.

Further, you will find 3 topics with step-by-step descriptions of the code.

##ReportinBot## 

Our objective is to provide a description of the major product metrics for the last 7 days.
The data will be reported via Telegram, so we have already installed the Telegram library and defined the chat ID.
In the report, there will be text that displays numbers for each metric yesterday, along with graphs that display dynamic data for each metric for the past 7 days.
All data is coming from the SQL server, so we will need a library to work with ClickHouse SQL language.
DAG will accomplish all of these steps.


##ETL##

Our objective is to obtain data about the activity of each user's category (age, gender, and OS (Android or iOS) from 2 separate database.
Activity includes messeges (sent, received), views, likes. 
We will merge every compounded table and upload it back to the SQL server to create a single table that contains the main data.


##Dashboard##

I attempted to demonstrate the possibilities of data visualization in this instance. 
I got the data by making a request to the SQL server and made some minor edits using Pandas.



I want to thank karpov.courses for the excellent course and the provided data.


<img width="1485" alt="Dashboard" src="https://github.com/ProkhodtsevKirill/Analyst/assets/160003420/6925456b-6146-4155-8219-4770e45025ca">
