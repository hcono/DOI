"""

Python script with functions for creating a DOI from a data catalogue entry

created by: Conor Hayes
Date: 24/10/2023



"""
# Contents 
# functions
# 1. Check database to see if any DOIs are required
# 2. Use the pubIDs to create an Datacite compatable XML for the dataset
# 3. Get a DOI and update the SQL tables


# Required libraries
import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import uuid
import base64
from datetime import date
from sqlalchemy import create_engine, text


# required variables
pubIDs = []


# 1. dois_required_check
# Check the Dataset table to identify the publication ids that require a DOI
# dependents:
#   pubIDs: empty list that contains pubIDs that require dois
#   pandas: read the sql qury
#   engine: sqlalchemy connection

def dois_required_check(engine):
    query_string = """SELECT pub.ID as 'PubID'
    FROM [dbo].[PUBLICATION] pub
    Right Join [dbo].[DATASET] ds on ds.PUBLICATION_ID = pub.ID
    WHERE pub.PUBLISHED = 1 
    AND pub.DOI_PUBLICATION_DATE is null
    AND pub.ID != 12551
    """

    try:
        # Test the connection by executing a simple query
        df = pd.read_sql_query(query_string, engine)

        for i in range(0, len(df)):
            pubIDs.append(df.iloc[i]['PubID'])
        print('query for doi check finished')

    except Exception as e:
        print(f"An error occurred: {e}")


# 2. createXML
# uses a sql function to create a DataiteXML and save the xml to to 'N:\PublishedDataLibrary\dataciteXML'
# depenedants:
#   engine: same as above
#   pubID: read from pubID list
#   destination_folder: folder that XML will be stored in
#   os
#   xml.etree.ElementTree as ET

def createXML(engine, pubID, destination_folder):
    if len(pubID) <= 0:
        print('All published files have DOIs\n\n')
    else:
        query = '''
        SELECT dbo.DataciteXmlById(%s) AS DataCiteXML
        '''%(pubID)
        try:
            # Execute the query and fetch the result
            with engine.connect() as connection:
                result = connection.execute(query)

                for row in result:
                    xml_data = row.DataCiteXML
                    root = ET.fromstring(xml_data)
                    # Add namespaces to the root element and its descendants
                    root.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
                    root.set('xmlns',"http://datacite.org/schema/kernel-4")
                    root.set('xsi:schemaLocation',"http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd")
                        # Write the modified XML to a file
                    xml_file_path = os.path.join(os.path.normpath(destination_folder),'DC%s.xml' %(pubID))
                    xml_path = os.path.normpath(xml_file_path)
                    #files.append(xml_path)
                    tree = ET.ElementTree(root)
                    tree.write(xml_path, encoding='utf-8', xml_declaration=True)

                    print('XML for %s was created and stored in xml_path'%pubID)  # This will print each row as a tuple
        except Exception as e:
            print(f"An error occurred: {e}")    


# so far we have a list of pubIDs and an XML in destination folder 
# next:
# 3. createDataciteDOI

# connect to datacite API to create a DOI
# connect to shortDOIto get a short DOI after long DOI is created
# DataCite API requirements:
#   3.1 generated UUID: 
#   3.2 base64 encode XML:
#   3.3 shortDOI requirements:
#   3. doi


# 3.1 getDOIstring
# creates a random UUID and appends to Marine Institute owned DOI starter '10.20393'
# dependants:
#   uuid: python library
def getDOIstring():
    myUUID = uuid.uuid4()
    doi = f'10.20393/{myUUID}'
    return doi


# 3.2 encode_xml_to_base64
# reads the created dataciteXMLs and converts them to base64 string to be used in Datacite API
# dependants:
#   destination_folder: used from above, need to get the file path of XML so they can be opened
#   pubID: to get XML of the correct pubID (allows for iteration)
# requirments:
#   os: to read file
#   base64

def encode_xml_to_base64(destination_folder, pubID):
    xml_file_path = os.path.join(os.path.normpath(destination_folder),'DC%s.xml' %(pubID))
    try:
        with open(xml_file_path, 'rb') as xml_file:
            # Read the XML content
            xml_content = xml_file.read()

            # Encode the XML content to Base64
            base64_encoded = base64.b64encode(xml_content).decode('utf-8')

            return base64_encoded

    except FileNotFoundError:
        return None
    

# 3.3 get_short_doi
# gets a short doi from the long doi using shortDOI.org API
# dependants:
#   doi: long doi that will be generated from datacite
# requirements:
#   requests: connect to shortdoi API
    
def get_short_doi(doi):
    short_doi_url = f"http://shortdoi.org/{doi}"
    response = requests.get(short_doi_url)
    if response.status_code == 200:
        short_doi = response.text.strip()
        start_index = short_doi.find('<div class="para">10/')
        end_index = short_doi.find('</div>', start_index)

        # Extract the short DOI value
        if start_index != -1 and end_index != -1:
            sho_doi = short_doi[start_index + len('<div class="para">'):end_index].strip()

            return sho_doi
        else:
            return None
    else:
        return response.status_code, response.text


# 3. createDataciteDOI
# get a DOI from datacite API, also updates dataset and publication tables in SQL server
# dependants:
#   destination_folder: same as above, to get the XML path for encoded_xml_to_base64 function
#   engine: same as above, allows for sqlalchmey connection to the database
#   pubID: sameas above, required to read XML, update tables
#   getDOIstring: function 3.1, required to get a generated doi that is used in the datacite API JSON
#   encoded_xml_to_base64: function 3.2, required for XML encoding for datacite API JSON
#   get_short_doi: function 3.3, required to create a short doi from Datacite API DOI
# Requirements:
#   os: connect to file path
#   requests: connect to API
#   sqlalchemy: text/create engine. allows connection to database
#   datetime:get current date for publised date

def createDataciteDOI(destination_folder, engine, pubID):
    
    xml_file_path = os.path.join(os.path.normpath(destination_folder),'DC%s.xml' %(pubID))
    doi = getDOIstring()
    encodedXML = encode_xml_to_base64(xml_file_path)
    
    # Set the API endpoint URL
    api_url = 'https://api.datacite.org/dois'
    # Set the headers
    headers = {
        'Content-Type': 'application/vnd.api+json',
        "Authorization": "Basic QkwuTUFSSU5FSUU6TWFyaW5lMTc="
    }
    # Set the metadata payload
    metadata = {
        'data': {
            'type': 'dois',
            'attributes': {
                'event': 'hide',
                'doi': doi,  # Replace with your DOI prefix
                'xml': encodedXML  # Replace with your XML content as base64
            }
        }
    }
    # Make the POST request
    response = requests.post(api_url, headers=headers, json=metadata)

    if response.status_code == 201:
        doi_data = response.json()
        doiDC = doi_data['data']['attributes']['doi']
        try:
            shortDOI = get_short_doi(doiDC)
            update_publication = text("UPDATE [dbo].[PUBLICATION] SET doi = :new_value1, DOI_PUBLICATION_DATE = :new_value2, shortDOI = :new_value3 WHERE id = :target_id; = :new_value1, column2 = :new_value2 WHERE id = :target_id")
            update_dataset = text("UPDATE [dbo].[DATASET] SET uuid = :new_value4 WHERE PUBLICATION_ID = :target_id2")
                # Replace 'your_table_name', 'column1', 'new_value1', 'clumn2', 'new_value2', and 'target_id' with your specific values

            with engine.connect() as connection:
                result1 = connection.execute(update_publication, new_value1="%s", new_value2="%s", new_value3="%s", target_id="%s")%(doiDC, date.today(), shortDOI, pubID)
                result2 = connection.execute(update_dataset, new_value4= "%s", target_id2 = pubID)

                # The above statement will update 'column1' and 'column2' in the row with 'id' equal to pubID
                # Make sure to adapt the query and parameters according to your table structure
            return f"Publication Table Rows affected: {result1.rowcount}, Dataset Table rows affected: {result2.rowcount}"

        except Exception as e:
            return f"An error occurred: {e}"

    else:
        return f'\nDOI creation failed for {pubID}.\n\tResponse code: {response.status_code}, Response body: {response.text}\n\tIf file has been created, manually upload the DataCiteXML from N:\PublishedDataLibrary\dataciteXML\n'
