#pip install clarifai_grpc

#importing deepface library and DeepFace
import mysql.connector
from datetime import datetime
import pandas as pd
import json
import operator


########################################################################################
# In this section, we set the user authentication, app ID, model details, and the URL
# of the image we want as an input. Change these strings to run your own example.
#######################################################################################

USER_ID = 'wj03cd9uuhnt'
# Your PAT (Personal Access Token) can be found in the portal under Authentification
PAT = '05d063165ded43c88d60773f22118cd1'
APP_ID = 'fe1e29f5552d430d8365fc782a446809'
# Change this to whatever image URL you want to process
IMAGE_URL = 'https://a0.muscache.com/im/pictures/user/c22d19f5-bf5f-48b3-a6b7-7f8abd5c20e4.jpg'
# This is optional. You can specify a model version or the empty string for the default
MODEL_VERSION_ID = ''

############################################################################
# YOU DO NOT NEED TO CHANGE ANYTHING BELOW THIS LINE TO RUN THIS EXAMPLE
############################################################################
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2

channel = ClarifaiChannel.get_grpc_channel()
stub = service_pb2_grpc.V2Stub(channel)

metadata = (('authorization', 'Key ' + PAT),)

userDataObject = resources_pb2.UserAppIDSet(user_id=USER_ID, app_id=APP_ID)


def getPredictionFromModel(MODEL_ID, url):
        post_model_outputs_response = stub.PostModelOutputs(
            service_pb2.PostModelOutputsRequest(
                user_app_id=userDataObject,  # The userDataObject is created in the overview and is required when using a PAT
                model_id=MODEL_ID,
                version_id=MODEL_VERSION_ID,  # This is optional. Defaults to the latest model version
                inputs=[
                    resources_pb2.Input(
                        data=resources_pb2.Data(
                            image=resources_pb2.Image(
                                url=url
                            )
                        )
                    )
                ]
            ),
            metadata=metadata
        )
        if post_model_outputs_response.status.code != status_code_pb2.SUCCESS:
            print(post_model_outputs_response.status)
            raise Exception("Post model outputs failed, status: " + post_model_outputs_response.status.description)

        # Since we have one input, one output will exist here
        output = post_model_outputs_response.outputs[0]

        # Uncomment this line to print the full Response JSON
        print(output.data)

        print("Predicted concepts:")
        
        predictedconcepts = {}
        for concept in output.data.concepts:
        predictedconcepts[concept.value] = {concept.name}

        print(predictedconcepts)
        return str(predictedconcepts.get(max(predictedconcepts)))
        

############################################################################
# Entry point for app. Get all the users information
############################################################################
print('start at: ' + str(datetime.now()))
mydb = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = "mysql@123",
            database = "airbnb2022",
            auth_plugin='mysql_native_password'
        )

cursor = mydb.cursor()
#query = "SELECT * FROM airbnb_listing_host where picture_large_url is not null limit 0, 1000;"
query = "SELECT * FROM airbnb_listing_users_sample;"
cursor.execute(query)
 
myresult = cursor.fetchall()
print(myresult)
count = 0
df = pd.DataFrame(myresult)
print(df.columns.values)
for x in myresult:
    print(x)
    url = x[3]

    #For Gender Prediction:
    GENDER_MODEL_ID = 'gender-demographics-recognition' 
    df.loc[count,[10]] = getPredictionFromModel(GENDER_MODEL_ID, url);


    #For ethnicity prediction
    ETHNICITY_MODEL_ID = 'ethnicity-demographics-recognition' 
    df.loc[count,[12]] = getPredictionFromModel(ETHNICITY_MODEL_ID, url);
    
    count+=1

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

print(myresult)
print(df)