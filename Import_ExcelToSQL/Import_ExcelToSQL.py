# This progrmme is used to import excel file into SQL database

import ftplib
import patoolib
import os
import glob
from zipfile import ZipFile
import pandas as pd
import pyodbc
import math


# Connection to PERFORMANCEDB01
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=PERFORMANCEDB01;'
                      'Database=Performance_NAK;'
                      'Trusted_Connection=yes;')
conn_performanceDB = conn.cursor()

conn_performanceDB.execute("TRUNCATE table Tehran_VIP_CC")
conn_performanceDB.execute("TRUNCATE table Tehran_NonVIP_CC")

# read excel file
Folder_list = glob.glob(r"\\dfs\fs\NPO\6. Performance\CC Project"+"\*.xlsx")



for i in range(len(Folder_list)):
    if len(Folder_list[i])>60:
        if Folder_list[i][39:53]=="MCI Management":
            Tehran_VIP_CC_Data = pd.read_excel(Folder_list[i])
    if len(Folder_list[i])>50:
        if Folder_list[i][39:46]=="Non Vip":
            Tehran_NonVIP_CC_Data = pd.read_excel(Folder_list[i])


Tehran_VIP_CC_Data_Table = pd.DataFrame(Tehran_VIP_CC_Data)
Tehran_VIP_CC_Data_Table.columns = [c.replace(' ', '_') for c in Tehran_VIP_CC_Data_Table.columns]

Tehran_NonVIP_CC_Data_Table = pd.DataFrame(Tehran_NonVIP_CC_Data)
Tehran_NonVIP_CC_Data_Table.columns = [c.replace(' ', '_') for c in Tehran_NonVIP_CC_Data_Table.columns]

for row in Tehran_VIP_CC_Data_Table.itertuples():
        CCID=row.CCID
        if str(CCID)=='nan':
            CCID=''
        SubscriberName=row.SubscriberName
        if str(SubscriberName)=='nan':
            SubscriberName=''
        SubscriberPhone=row.SubscriberPhone
        if str(SubscriberPhone)=='nan':
            SubscriberPhone=''
        SubscriberAddress=row.SubscriberAddress
        if str(SubscriberAddress)=='nan':
            SubscriberAddress=''
        LatitudeValue=row.LatitudeValue
        if str(LatitudeValue)=='nan':
            LatitudeValue=''
        LongitudeValue=row.LongitudeValue
        if str(LongitudeValue)=='nan':
            LongitudeValue=''
        SourceofComplaint=row.SourceofComplaint
        if str(SourceofComplaint)=='nan':
            SourceofComplaint=''
        RNC=row.RNC
        if str(RNC)=='nan':
            RNC=''
        NPOstatus=row.NPOstatus
        if str(NPOstatus)=='nan':
            NPOstatus=''
        Create_Date=row.Create_Date
        if str(Create_Date)=='NaT':
            Create_Date=''
        AssignComment=row.AssignComment
        if str(AssignComment)=='nan':
            AssignComment=''
        SolutionCategory=row.SolutionCategory
        if str(SolutionCategory)=='nan':
            SolutionCategory=''
        PendingSite=row.PendingSite
        if str(PendingSite)=='nan':
            PendingSite=''
        ResponsibleTeam=row.ResponsibleTeam
        if str(ResponsibleTeam)=='nan':
            ResponsibleTeam=''
        CustomerComplaintRef=row.CustomerComplaintRef
        if str(CustomerComplaintRef)=='nan':
            CustomerComplaintRef=''
        ReadyToCloseDate=row.ReadyToCloseDate
        if str(ReadyToCloseDate)=='NaT':
            ReadyToCloseDate=''
        ResolveDate=row.ResolveDate
        if str(ResolveDate)=='NaT':
            ResolveDate=''


        conn_performanceDB.execute('''
        INSERT INTO Tehran_VIP_CC (CCID,	SubscriberName,	SubscriberPhone,	SubscriberAddress,	LatitudeValue,	LongitudeValue,	SourceofComplaint,	RNC,	NPOstatus,	[Create Date],	AssignComment,	SolutionCategory,	PendingSite,	ResponsibleTeam,	CustomerComplaintRef,	ReadyToCloseDate,	ResolveDate)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''',
        CCID,
        SubscriberName,
        SubscriberPhone,
        SubscriberAddress,
        LatitudeValue,
        LongitudeValue,
        SourceofComplaint,
        RNC,
        NPOstatus,
        Create_Date,
        AssignComment,
        SolutionCategory,
        PendingSite,
        ResponsibleTeam,
        CustomerComplaintRef,
        ReadyToCloseDate,
        ResolveDate


        )
        conn.commit()
        

for row in Tehran_NonVIP_CC_Data_Table.itertuples():
        CCID=row.CCID
        if str(CCID)=='nan':
            CCID=''
        SubscriberName=row.SubscriberName
        if str(SubscriberName)=='nan':
            SubscriberName=''
        SubscriberPhone=row.SubscriberPhone
        if str(SubscriberPhone)=='nan':
            SubscriberPhone=''
        SubscriberAddress=row.SubscriberAddress
        if str(SubscriberAddress)=='nan':
            SubscriberAddress=''
        LatitudeValue=row.LatitudeValue
        if str(LatitudeValue)=='nan':
            LatitudeValue=''
        LongitudeValue=row.LongitudeValue
        if str(LongitudeValue)=='nan':
            LongitudeValue=''
        SourceofComplaint=row.SourceofComplaint
        if str(SourceofComplaint)=='nan':
            SourceofComplaint=''
        RNC=row.RNC
        if str(RNC)=='nan':
            RNC=''
        NPOstatus=row.NPOstatus
        if str(NPOstatus)=='nan':
            NPOstatus=''
        Create_Date=row.Create_Date
        if str(Create_Date)=='NaT':
            Create_Date=''
        AssignComment=row.AssignComment
        if str(AssignComment)=='nan':
            AssignComment=''
        SolutionCategory=row.SolutionCategory
        if str(SolutionCategory)=='nan':
            SolutionCategory=''
        PendingSite=row.PendingSite
        if str(PendingSite)=='nan':
            PendingSite=''
        ResponsibleTeam=row.ResponsibleTeam
        if str(ResponsibleTeam)=='nan':
            ResponsibleTeam=''
        CustomerComplaintRef=row.CustomerComplaintRef
        if str(CustomerComplaintRef)=='nan':
            CustomerComplaintRef=''
        ReadyToCloseDate=row.ReadyToCloseDate
        if str(ReadyToCloseDate)=='NaT':
            ReadyToCloseDate=''
        ResolveDate=row.ResolveDate
        if str(ResolveDate)=='NaT':
            ResolveDate=''


        conn_performanceDB.execute('''
        INSERT INTO Tehran_NonVIP_CC (CCID,	SubscriberName,	SubscriberPhone,	SubscriberAddress,	LatitudeValue,	LongitudeValue,	SourceofComplaint,	RNC,	NPOstatus,	[Create Date],	AssignComment,	SolutionCategory,	PendingSite,	ResponsibleTeam,	CustomerComplaintRef,	ReadyToCloseDate,	ResolveDate)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''',
        CCID,
        SubscriberName,
        SubscriberPhone,
        SubscriberAddress,
        LatitudeValue,
        LongitudeValue,
        SourceofComplaint,
        RNC,
        NPOstatus,
        Create_Date,
        AssignComment,
        SolutionCategory,
        PendingSite,
        ResponsibleTeam,
        CustomerComplaintRef,
        ReadyToCloseDate,
        ResolveDate


        )
        conn.commit()
        
conn.close()


# Delete Files After Upload in Datebase
File_Count=len(Folder_list)
for i in range(File_Count):
    if len(Folder_list[i])>60:
        if Folder_list[i][39:53]=="MCI Management":
            os.unlink(Folder_list[i])
    if len(Folder_list[i])>50:
        if Folder_list[i][39:46]=="Non Vip":
            os.unlink(Folder_list[i])