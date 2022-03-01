import pandas as pd
import os
import glob
import math
import datetime as datetime


# Helper functions
def getMonth(df):

    """
    This function get the month from one of the koku reports. It is used for the Credit Suisse Billing report but also
    to know which file, to append the newly generated data.
    """
    
    date = df['report_period_start'].tolist()[0].split(" ")[0]
    datetime_  = datetime.datetime.strptime(date, '%Y-%m-%d')
    month = datetime_.month
    return month

def getYear(df):
    
    """
    This function get the month from one of the koku reports. It is used for the Credit Suisse Billing report
    """

    date = df['report_period_start'].tolist()[0].split(" ")[0]
    datetime_  = datetime.datetime.strptime(date, '%Y-%m-%d')
    year = datetime_.year
    return year

# Getting the set of namespaces from the pod dataframe
def getNamespacesSet(df):
    
    """
    This functions returns a set (list of elements without duplicates) of namespaces from the Koku reports.
    """
    
    namespaces_list = df['namespace'].tolist()
    namespaces_list_cleaned = [x for x in namespaces_list if str(x) != 'nan'] # removing nan
    namespaces_set = set(namespaces_list_cleaned)
    return namespaces_set

# Total amount of minutes by report
def getTotalMinutes(df, namespace):
    
    """
    return the total amount of minutes a namespaces was in the cluster in the period of time of the report.
    Currently we know that each row, covers a timeframe of an hours, so we can calculate the total amount of minutes
    n_row *60. But this may need to be adapted if things change in the future.
    """

    # /!\ Works only because we know that the current timeframe is of 1 hour.
    minutes = df[df['namespace'] == namespace].shape[0] * 60

    return minutes

# Total amount of seconds by report
def getTotalSeconds(df, namespace):
    
    """
    return the total amount of seconds a namespaces was in the cluster in the period of time of the report.
    Currently we know that each row, covers a timeframe of an hours, so we can calculate the total amount of minutes
    n_row *3600. But this may need to be adapted if things change in the future.
    """

    # /!\ Works only because we know that the current timeframe is of 1 hour.
    seconds = df[df['namespace'] == namespace].shape[0] * 3600

    return seconds


def createMeteringReportDataframe():
    
    '''
     Creates the structure of the Credit Suisse Billing report with the corresponding columns.
    
     Consumption_Type Sender_DomainID Sender_Region Sender_ObjectID Sender_OptionID Receiver_Office
     Evidence1_DomainID Evidence1 Evidence2_DomainID Evidence2 Evidence3_DomainID Evidence3
     Evidence4_DomainID Evidence4 Unweighted_Volume Amount Currency Month Year Scenario Booking_Costtype
    
    '''
    
    
    metering_report_date = pd.DataFrame(columns=['Consumption_Type','Sender_DomainID','Sender_Region','Sender_ObjectID','Sender_OptionID','Receiver_Office','Evidence1_DomainID','Evidence1','Evidence2_DomainID','Evidence2','Evidence3_DomainID','Evidence3','Evidence4_DomainID','Evidence4','Unweighted_Volume','Amount','Currency','Month','Year','Scenario','Booking_Costtype'])
    metering_report_date["Month"] = metering_report_date["Month"].astype(str)
    metering_report_date["Year"] = metering_report_date["Year"].astype(str)
    metering_report_date["Scenario"] = metering_report_date["Scenario"].astype(str)
    metering_report_date["Year"] = metering_report_date["Year"].astype(str)
    metering_report_date["Amount"] = metering_report_date["Amount"].astype(str)
    metering_report_date["Currency"] = metering_report_date["Currency"].astype(str)

    return metering_report_date


# works only for namespace dataframe
def getCostCenter_dic(df):

    '''
    ICTO or COST CENTER depends on the cs_namespace_type label
    +--------------------+-------------+
    | Evidence1_DomainID | Namespace   |
    +--------------------+-------------+
    | ICTO               | application |
    +--------------------+-------------+
    | COST CENTER        | personal    |
    +--------------------+-------------+

    This is available as annotation billing.ocp.caas.csg.com/evidence1 for application namespace
    Since Koku operator uses labels instead of labels, this needs to be change from Credit Suisse side.
    Once change, someone from Credit Suisse, will need to update this function, with the correct label.
    billing.ocp.caas.csg.com/evidence1 annotation
    '''


    # Getting the labels for the Cost Center for each namespace
    # Every namespace will be associate with a cost center, that will be used later in the Credit Suisse Report Billing.
    namespace_label_dic = {}
    cost_center_dic = {}

    namespace_namespace_set = getNamespacesSet(df)
    # Iteration over all the namespaces, to check if they have a given label.
    for namespace in namespace_namespace_set:
        ''''label_str = label_controller_tools_k8s_io:1.0|label_openshift_io_cluster_monitoring:true'''
        # /!\ /!\ /!\
        # Will need to be changed to "billing.ocp.caas.csg.com/evidence1 annotation"
        label_list = df.loc[((df['namespace_labels'].str.contains('label_openshift_io_cluster_monitoring') ) & (df['namespace'] == namespace ))].namespace_labels.tolist()

        if len(label_list) > 0:
            label_str = label_list[0]
            ''''evidence1 = true'''
            label_str_processed = label_str.split("|")
            # Example: label_str_processed = ['label_openshift_io_cluster_monitoring:true', 'label_openshift_io_run_level:0']
            label_str_processed = [s for s in label_str_processed if "label_openshift_io_cluster_monitoring:true" in s]
            evidence1 = label_str_processed[0].split(":")[-1]
            cost_center_dic[namespace] = 'ICTO'

        else:
            # To be adapted with CS specifications
            evidence1 = 'false'
            cost_center_dic[namespace] = 'COST CENTER'

        namespace_label_dic[namespace] = evidence1

    return [namespace_label_dic,cost_center_dic]


def appendDataframesWithGoodFormat(df1,df2):
    
    '''
    Once the two dataframe appended, usually, one dataframe from a csv file that is already in the Credit Suisse format and the
    other freshly formated from a newly Koku report, we need to merge the data, summing up the duplicated values of most of the
    CS metrics except for the volume metric where we need to calculate the average value.
    '''
        
    report_df = df1.append(df2)
    
    report_df["Amount"] = ''
    report_df["Currency"] = ''
    report_df["Evidence3"] = report_df["Evidence3"].astype(float)
    report_df["Evidence4"] = report_df["Evidence4"].astype(float)
    report_df["Unweighted_Volume"] = report_df["Unweighted_Volume"].astype(float)
    report_df["Evidence1"] = report_df["Evidence1"].apply(lambda x: str(x).lower())

    # Splitting the appended dataframe into a dataframe with only volume metrics and another with only non volume metrics
    report_df_without_volume = report_df[report_df['Evidence2_DomainID'] != 'Application Namespace_Storage']
    # Summing up all the values of the duplicates
    report_df_without_volume = report_df_without_volume.groupby(['Consumption_Type','Sender_DomainID','Sender_Region','Sender_ObjectID','Sender_OptionID','Receiver_Office','Evidence1_DomainID','Evidence1','Evidence2_DomainID','Evidence2','Evidence3_DomainID','Evidence4_DomainID','Amount','Currency','Month','Year','Scenario','Booking_Costtype'], as_index=False).sum()
    # Reordering the columns so that they match with the Credit Suisse Billing report structure
    report_df_without_volume = report_df_without_volume[['Consumption_Type','Sender_DomainID','Sender_Region','Sender_ObjectID','Sender_OptionID','Receiver_Office','Evidence1_DomainID','Evidence1','Evidence2_DomainID','Evidence2','Evidence3_DomainID','Evidence3','Evidence4_DomainID','Evidence4','Unweighted_Volume','Amount','Currency','Month','Year','Scenario','Booking_Costtype']]

    # Splitting the appended dataframe into a dataframe with only volume metrics and another with only volume metrics
    report_df_only_volume = report_df[report_df['Evidence2_DomainID'] == 'Application Namespace_Storage']
    
    report_df_only_volume = report_df_only_volume.groupby(['Consumption_Type','Sender_DomainID','Sender_Region','Sender_ObjectID','Sender_OptionID','Receiver_Office','Evidence1_DomainID','Evidence1','Evidence2_DomainID','Evidence2','Evidence3_DomainID','Evidence4_DomainID','Amount','Currency','Month','Year','Scenario','Booking_Costtype'], as_index=False).mean()
    # Reordering the columns so that they match with the Credit Suisse Billing report structure
    report_df_only_volume = report_df_only_volume[['Consumption_Type','Sender_DomainID','Sender_Region','Sender_ObjectID','Sender_OptionID','Receiver_Office','Evidence1_DomainID','Evidence1','Evidence2_DomainID','Evidence2','Evidence3_DomainID','Evidence3','Evidence4_DomainID','Evidence4','Unweighted_Volume','Amount','Currency','Month','Year','Scenario','Booking_Costtype']]

    # Appending again the two previously splitted dataframes
    appended_report = report_df_without_volume.append(report_df_only_volume)

    return appended_report


def koku_to_cs(namespace_data_raw, node_data_raw, pod_data_raw, volume_data_raw):
    
    '''
    Main function, that will use the 4 generated csv files from the Koku Metering Operator, to create a csv report file with
    the Credit Suisse Metering Billing format.
    '''
    
    bytesToGigabytes = 0.000000000931

    ### Processing namespace data
    namespace_data = namespace_data_raw.drop_duplicates(['report_period_start', 'report_period_end','namespace','namespace_labels'])
    namespace_data["namespace_labels"] = namespace_data["namespace_labels"].astype(str)
    namespace_data = namespace_data.groupby(['report_period_start', 'report_period_end','namespace'])['namespace_labels'].apply(lambda x: '|'.join(x)).reset_index()


    ### Processing node data
    node_data = node_data_raw.drop_duplicates(['node','node_labels'])

    ### Processing pod data
    # Create the cpu_burst column
    pod_data_raw['cpu_burst_sum_minutes'] = pod_data_raw.apply(lambda row: (row.pod_usage_cpu_core_seconds - row.pod_request_cpu_core_seconds)/60 # Minutes
                                                                            if row.pod_usage_cpu_core_seconds > row.pod_request_cpu_core_seconds and (row.pod_usage_cpu_core_seconds and row.pod_request_cpu_core_seconds) > 0 and (row.pod_usage_cpu_core_seconds - row.pod_request_cpu_core_seconds > 60)
                                                                            else (1 
                                                                            if row.pod_usage_cpu_core_seconds > row.pod_request_cpu_core_seconds and (row.pod_usage_cpu_core_seconds and row.pod_request_cpu_core_seconds) > 0 and (row.pod_usage_cpu_core_seconds - row.pod_request_cpu_core_seconds <= 60) 
                                                                            else 0), axis=1)
    
    # Create the memory_burst column
    pod_data_raw['memory_burst_sum_minutes'] = pod_data_raw.apply(lambda row: (row.pod_usage_memory_byte_seconds - row.pod_request_memory_byte_seconds)/60 * bytesToGigabytes
                                                                            if row.pod_usage_memory_byte_seconds > row.pod_request_memory_byte_seconds and (row.pod_usage_memory_byte_seconds and row.pod_request_memory_byte_seconds) > 0 and (row.pod_usage_memory_byte_seconds - row.pod_request_memory_byte_seconds > 60)
                                                                            else (1 * bytesToGigabytes
                                                                            if row.pod_usage_memory_byte_seconds > row.pod_request_memory_byte_seconds and (row.pod_usage_memory_byte_seconds and row.pod_request_memory_byte_seconds) > 0 and (row.pod_usage_memory_byte_seconds - row.pod_request_memory_byte_seconds <= 60) 
                                                                            else 0), axis=1)
    
    
    # grouping the dataframe based namespaces and report period and get the sum per column.
    pod_data_raw["pod_request_cpu_core_minutes"] = pod_data_raw.apply(lambda row: row.pod_request_cpu_core_seconds/60, axis=1)
    pod_data_raw["pod_request_memory_gigabyte_minutes"] = pod_data_raw.apply(lambda row: (row.pod_request_memory_byte_seconds/60)* bytesToGigabytes, axis=1)

    pod_data = pod_data_raw.groupby(["report_period_start", "report_period_end","namespace"], as_index=False).sum()
    pod_data = pod_data.rename(columns={'pod_request_memory_byte_seconds': 'pod_request_memory_gigabyte_seconds', 'pod_usage_memory_byte_seconds': 'pod_usage_memory_gigabyte_seconds','pod_limit_memory_byte_seconds': 'pod_limit_memory_gigabyte_seconds'})


    ### Processing volume data
    volume_data_raw["volume_request_storage_gigabytes"] = volume_data_raw.apply(lambda row: (row.volume_request_storage_byte_seconds * bytesToGigabytes)/3600 if row.persistentvolumeclaim_capacity_bytes > 0 else 0, axis=1)
    #volume_data = volume_data_raw.groupby(["report_period_start", "report_period_end","namespace"], as_index=False).mean()
    volume_data = volume_data_raw
    
    # Getting namespace set
    pod_namespace_set = getNamespacesSet(pod_data)
    volume_namespace_set = getNamespacesSet(volume_data)
    namespace_namespace_set = getNamespacesSet(namespace_data)
    
    # Creating Metering Report Dataframe
    metering_report_date = createMeteringReportDataframe()

    # Creating Cost Center Dictionaries  
    namespace_label_dic = getCostCenter_dic(namespace_data)[0]
    cost_center_dic = getCostCenter_dic(namespace_data)[1]
    
    
    ####################################################################################################################
    ################################ Setting variable for the report ###################################################
    ####################################################################################################################
    # Constant Variables
    consumption_type = 'Technical Volume'
    Sender_DomainID = 'PROD'
    sender_ObjectId = 'P-CAAS'
    Currency = ''
    Amount = ''
    Scenario = 1
    Booking_Costtype = 'Product'

    # Dictionaires
    sender_OptionId = {'Application Namespace_Namespace':'OCPSSAN','Application Namespace_CPU Burst Time':'OCPSSANCBT','Application Namespace_CPU Reserved Time':'OCPSSANCRT','Application Namespace_Memory Burst Time':'OCPSSANMBT','Application Namespace_Memory Reserved Time':'OCPSSANMRT','Application Namespace_Storage':'OCPSSANSTO','Personal Namespace':'OCPSSPN'}
    Evidence4_DomainID = {'Personal Namespace':'Minutes','Application Namespace_Storage':'GB','Application Namespace_CPU Burst Time':'Minutes','Application Namespace_CPU Reserved Time':'Minutes','Application Namespace_Memory Burst Time':'Minutes','Application Namespace_Memory Reserved Time':'Minutes','Application Namespace_Namespace':'Minutes'}
    

    # Variables
    Evidence1_DomainID = ''
    Evidence1 = '' # Cost Center
    Evidence2_DomainID = ''
    Evidence2 = '' # Namesapce
    Evidence3_DomainID = 'Quantity'
    Evidence3 = '' # Value
    Evidence4 = Evidence3
    Unweighted_Volume = Evidence4
    Month = ''
    year = ''
    Sender_Region = 'Switzerland' # Currently don't know how to get it
    Receiver_Office = 'Switzerland' # Currently don't know how to get it
    
    ####################################################################################################################
    ################################ Application Namespace_Namespace ###################################################
    ####################################################################################################################

    Evidence2_DomainID = 'Application Namespace_Namespace'
    Evidence4_DomainID_key = Evidence4_DomainID[Evidence2_DomainID]
    sender_OptionId_key = sender_OptionId[Evidence2_DomainID]
    Month = getMonth(pod_data)
    Year = getYear(pod_data)

    for namespace in namespace_namespace_set:

        minutes = getTotalMinutes(namespace_data_raw,namespace)

        if minutes > 0:
            Evidence3 = minutes

        else:
            continue

        if namespace in namespace_label_dic:
            Evidence1 = namespace_label_dic[namespace]
        else:
            Evidence1 = ''


        Evidence1_DomainID = cost_center_dic[namespace]
        Evidence2 = namespace
        Evidence4 = Evidence3
        Unweighted_Volume = Evidence4

        new_row = {'Consumption_Type':consumption_type,'Sender_DomainID':Sender_DomainID,'Sender_Region':Sender_Region,'Sender_ObjectID':sender_ObjectId,'Sender_OptionID':sender_OptionId_key,'Receiver_Office':Receiver_Office,'Evidence1_DomainID':Evidence1_DomainID,'Evidence1':Evidence1,'Evidence2_DomainID':Evidence2_DomainID,'Evidence2':Evidence2,'Evidence3_DomainID':Evidence3_DomainID,'Evidence3':Evidence3,'Evidence4_DomainID':Evidence4_DomainID_key,'Evidence4':Evidence4,'Unweighted_Volume':Unweighted_Volume,'Amount':Amount,'Currency':Currency,'Month':Month,'Year':Year,'Scenario':Scenario,'Booking_Costtype':Booking_Costtype}

        metering_report_date = metering_report_date.append(new_row, ignore_index=True)
        
        
        
    ####################################################################################################################
    ################################ Application Namespace_CPU Burst Time ##############################################
    ################################ Application Namespace_CPU Reserved Time ###########################################
    ################################ Application Namespace_Memory Burst Time ###########################################
    ################################ Application Namespace_Memory Reserved Time ########################################
    ####################################################################################################################

    Evidence2_DomainIDs = ['Application Namespace_CPU Burst Time','Application Namespace_CPU Reserved Time','Application Namespace_Memory Burst Time','Application Namespace_Memory Reserved Time']
    
    Evidence2_DomainID_column_dic = {'Application Namespace_CPU Burst Time':'cpu_burst_sum_minutes','Application Namespace_CPU Reserved Time':'pod_request_cpu_core_minutes','Application Namespace_Memory Burst Time':'memory_burst_sum_minutes','Application Namespace_Memory Reserved Time':'pod_request_memory_gigabyte_minutes'}
    
    
    
    for Evidence2_DomainID in Evidence2_DomainIDs:

        Evidence4_DomainID_key = Evidence4_DomainID[Evidence2_DomainID]
        sender_OptionId_key = sender_OptionId[Evidence2_DomainID]

        # Function for metric collection
        for namespace in pod_namespace_set:

            metric = pod_data.loc[pod_data['namespace'] == namespace][Evidence2_DomainID_column_dic[Evidence2_DomainID]].iloc[0]

            if metric > 0:
                    Evidence3 = round(metric,1)

            else:
                continue

            if namespace in namespace_label_dic:
                Evidence1 = namespace_label_dic[namespace]
            else:
                Evidence1 = ''


            Evidence1_DomainID = cost_center_dic[namespace]
            Evidence2 = namespace
            Evidence4 = Evidence3
            Unweighted_Volume = Evidence4

            new_row = {'Consumption_Type':consumption_type,'Sender_DomainID':Sender_DomainID,'Sender_Region':Sender_Region,'Sender_ObjectID':sender_ObjectId,'Sender_OptionID':sender_OptionId_key,'Receiver_Office':Receiver_Office,'Evidence1_DomainID':Evidence1_DomainID,'Evidence1':Evidence1,'Evidence2_DomainID':Evidence2_DomainID,'Evidence2':Evidence2,'Evidence3_DomainID':Evidence3_DomainID,'Evidence3':Evidence3,'Evidence4_DomainID':Evidence4_DomainID_key,'Evidence4':Evidence4,'Unweighted_Volume':Unweighted_Volume,'Amount':Amount,'Currency':Currency,'Month':Month,'Year':Year,'Scenario':Scenario,'Booking_Costtype':Booking_Costtype}

            metering_report_date = metering_report_date.append(new_row, ignore_index=True)
                
            
    ####################################################################################################################
    ################################ Application Namespace_Storage #####################################################
    ####################################################################################################################

    Evidence2_DomainID = 'Application Namespace_Storage'
    Evidence4_DomainID_key = Evidence4_DomainID[Evidence2_DomainID]
    sender_OptionId_key = sender_OptionId[Evidence2_DomainID]

    if volume_data.shape[0] > 0: #We check that the dataframe is not empty which can happen

        for namespace in volume_namespace_set:

            volume_request_storage_gigabytes = volume_data.loc[((volume_data['namespace'] == namespace) & (volume_data['volume_request_storage_gigabytes'] > 0))].mean().volume_request_storage_gigabytes

            if volume_request_storage_gigabytes > 0:
                Evidence3 = volume_request_storage_gigabytes
            else:
                continue

            if namespace in namespace_label_dic:
                Evidence1 = namespace_label_dic[namespace]
            else:
                Evidence1 = ''

            Evidence1_DomainID = cost_center_dic[namespace]
            Evidence2 = namespace
            Evidence4 = Evidence3
            Unweighted_Volume = Evidence4

            new_row = {'Consumption_Type':consumption_type,'Sender_DomainID':Sender_DomainID,'Sender_Region':Sender_Region,'Sender_ObjectID':sender_ObjectId,'Sender_OptionID':sender_OptionId_key,'Receiver_Office':Receiver_Office,'Evidence1_DomainID':Evidence1_DomainID,'Evidence1':Evidence1,'Evidence2_DomainID':Evidence2_DomainID,'Evidence2':Evidence2,'Evidence3_DomainID':Evidence3_DomainID,'Evidence3':Evidence3,'Evidence4_DomainID':Evidence4_DomainID_key,'Evidence4':Evidence4,'Unweighted_Volume':Unweighted_Volume,'Amount':Amount,'Currency':Currency,'Month':Month,'Year':Year,'Scenario':Scenario,'Booking_Costtype':Booking_Costtype}

            metering_report_date = metering_report_date.append(new_row, ignore_index=True)
            
            
    return metering_report_date
    
    
    