# interact -gpu 
# singularity shell --nv /ocean/containers/ngc/tensorflow/tensorflow_23.04-tf2-py3.sif
# python
#  import tensorflow


import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import sklearn.metrics as metrics 
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import confusion_matrix,ConfusionMatrixDisplay
from xgboost import XGBClassifier


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
##
# reading in data and cleaning it
ddint= pd.read_csv('ddinter_downloads_code_A.csv')
ddint= ddint.drop(columns=['DDInterID_A','DDInterID_B'] ) 
# dropping the data with unknow 
ddint=ddint[ddint['Level']!='Unknown']

##
# making one hot encoder matrix for deug a and drug b 
onehot_encoder = OneHotEncoder(handle_unknown='ignore')
l = onehot_encoder.fit_transform(ddint.drop(columns=['Level'])).toarray()
l=pd.DataFrame(l)
df=pd.merge(l,ddint['Level'],left_index=True,right_index=True)

# to relabel the classes 
def encode_severity_levels(df):
    # Create a mapping dictionary
    level_mapping = {
        'Moderate': 1,
        'Minor': 0,
        'Major': 2
    }
    
    # Create a copy to avoid modifying the original dataframe
    df_encoded = df.copy()
    
    # Apply the mapping to the Level column
    df_encoded['Level'] = df_encoded['Level'].map(level_mapping)
    
    return pd.DataFrame(df_encoded)

## using xgboost with weights
df=encode_severity_levels(df)
train_x,test_x,train_y,test_y= train_test_split(df.drop(columns=['Level']), df['Level'], test_size=.2)
 #second split for validation 

train_x, val_x, train_y, val_y = train_test_split(train_x, train_y, test_size=0.2, random_state=42)
# for weights
class_counts = np.bincount(train_y)
class_weights = {i: len(train_y) / (len(class_counts) * count) for i, count in enumerate(class_counts)}
# creating model
model= XGBClassifier( random_state=42, num_class=3, objective='multi:softmax')
# adding weights n running model
sample_weights = np.array([class_weights[y] for y in train_y])
model.fit(train_x, train_y, sample_weight=sample_weights)

# analysing model 
predictions=model.predict(test_x)
print(metrics.classification_report(test_y,predictions))
d=ConfusionMatrixDisplay(confusion_matrix(test_y,predictions))
display_labels=(['Class 0', 'Class 1'])
d.plot(cmap=plt.cm.Blues)
plt.title('Confusion Matrix')
plt.savefig('/jet/home/mmarius/project/xg_with_weight_cm.png')
plt.clf()


# Officail model accuracy 
metrics.accuracy_score(model.predict(val_x),val_y)



# Creating model without weight for comparison 

model= XGBClassifier(random_state=42, num_class=3, objective='multi:softmax')
# adding weights n running model
model.fit(train_x, train_y)

# analysing model 
predictions=model.predict(test_x)
print(metrics.classification_report(test_y,predictions))
d=ConfusionMatrixDisplay(confusion_matrix(test_y,predictions))
display_labels=(['Class 0', 'Class 1','Class 2'])
d.plot(cmap=plt.cm.Blues)
plt.title('Confusion Matrix')
plt.savefig('/jet/home/mmarius/project/xg_no_weight_cm.png')
plt.clf()


# Officail model accuracy 
metrics.accuracy_score(model.predict(val_x),val_y)

# As we can see the model performs worse with weights but overall is better because 
# it performs well with the minor classes , can tweak the weights to make this better since we 
# techincally know teh 'ceiling' of our model


# But before that trying to use all the features and making xgboost model with weights

drug_acid= pd.read_csv('drug_cids.csv')
pchem= pd.read_csv('pubchem_data.csv')
# changing name to make it make sense for merging
pchem['CIDs']=pchem['cid']
pchem=pchem.drop(columns=['cid'])
# merging data frames 
drug_features=pd.merge(drug_acid,pchem,right_on='CIDs',left_on='CIDs')
drug_features.drop(columns=['cmpdname','cmpdsynonym','mf','meshheadings','annothits','aids','cidcdate','sidsrcname','depcatg','annotation'])

# run this seperatly to see which columns i removed 
# columns to drop (besides drug name and cid )
filter=~drug_features.columns.isin(drug_features.describe().columns)
drug_features.columns[filter]

# removing unwanted columns
drug_features=pd.merge(drug_acid,pchem,right_on='CIDs',left_on='CIDs')
drug_features=drug_features.drop(columns=['cmpdname','cmpdsynonym','mf','meshheadings','annothits','aids','cidcdate','sidsrcname','depcatg','annotation','inchi', 'isosmiles',
       'canonicalsmiles', 'inchikey', 'iupacname'])

## might have done an error with merging but more than likely have more features because of the diff
# properties for each drug.
first_df=pd.merge(ddint,drug_features,left_on='Drug_A', right_on='Drug Name')
max_df= pd.merge(first_df,drug_features,left_on='Drug_B', right_on='Drug Name')
max_df=max_df.drop(columns=['Drug Name_x','Drug Name_y','CIDs_x','CIDs_y'])
# we have more data now because drug_features has drugs with the same names with diff properties 
# merge twice to get the features for both drug a and drug b 



# Prepeping data and Training to fitting model 
max_df= encode_severity_levels(max_df)
l = onehot_encoder.transform(max_df[['Drug_A','Drug_B']]).toarray()
l=pd.DataFrame(l)
fin_df=pd.merge(l,max_df.drop(columns=['Drug_A','Drug_B']),left_index=True,right_index=True)

# making model 


train_x,test_x,train_y,test_y= train_test_split(fin_df.drop(columns=['Level']), fin_df['Level'], test_size=.2)
 #second split to get validation  

train_x, val_x, train_y, val_y = train_test_split(train_x, train_y, test_size=0.2, random_state=42)

class_counts = np.bincount(train_y)
class_weights = {i: len(train_y) / (len(class_counts) * count) for i, count in enumerate(class_counts)}
model= XGBClassifier( random_state=42, num_class=3, objective='multi:softmax')

sample_weights = np.array([class_weights[y] for y in train_y])
model.fit(train_x, train_y, sample_weight=sample_weights)

# analyszing model 
predictions=model.predict(test_x)
print(metrics.classification_report(test_y,predictions))
d=ConfusionMatrixDisplay(confusion_matrix(test_y,predictions))
display_labels=(['Class 0', 'Class 1'])
d.plot(cmap=plt.cm.Blues)
plt.title('Confusion Matrix')
plt.savefig('/jet/home/mmarius/project/xg_allfeaturs_with_weight_cm.png')
plt.clf()


# Officail model accuracy 
metrics.accuracy_score(model.predict(val_x),val_y)

# based on this our model is doing amazing with the minor classes but we're penalizing this a little tooo much 
# can improve model if we find a way to penalize the majority class less 




# creating the model with all the features but no weights 
model= XGBClassifier( random_state=42, num_class=3, objective='multi:softmax')
model.fit(train_x, train_y)

# analyszing model 
predictions=model.predict(test_x)
print(metrics.classification_report(test_y,predictions))
d=ConfusionMatrixDisplay(confusion_matrix(test_y,predictions))
display_labels=(['Class 0', 'Class 1'])
d.plot(cmap=plt.cm.Blues)
plt.title('Confusion Matrix')
plt.savefig('/jet/home/mmarius/project/xg_allfeaturs_no_weight_cm.png')
plt.clf()

# Officail model accuracy 
metrics.accuracy_score(model.predict(val_x),val_y)


# trying to play with the weights to improve moderate class( class 1)


# just increased the weight for class 1
class_weights[1]=1.4
model= XGBClassifier( random_state=42, num_class=3, objective='multi:softmax')

sample_weights = np.array([class_weights[y] for y in train_y])
model.fit(train_x, train_y, sample_weight=sample_weights)

# analyszing model 
predictions=model.predict(test_x)
print(metrics.classification_report(test_y,predictions))
d=ConfusionMatrixDisplay(confusion_matrix(test_y,predictions))
display_labels=(['Class 0', 'Class 1'])
d.plot(cmap=plt.cm.Blues)
plt.title('Confusion Matrix')
plt.savefig('/jet/home/mmarius/project/xg_allfeaturs_edited_weight_cm.png')
plt.clf()


# Officail model accuracy 
metrics.accuracy_score(model.predict(val_x),val_y)


# as a result we got a better model but the smaller classes suffered a little bit. 
# next steps is to hyper paramterize with weights and with actual parameters for xgboost
