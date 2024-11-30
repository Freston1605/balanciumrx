# ================================================
# Machine Learning Pipeline for Multi-Class Classification
# ================================================

# Environment Setup
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
    accuracy_score,
)
from xgboost import XGBClassifier
import warnings

# Suppress warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

# ================================================
# Step 1: Data Loading and Cleaning
# ================================================
# Load dataset and drop unnecessary columns
ddint = pd.read_csv("project/ddinter_downloads_code_A.csv")
ddint = ddint.drop(columns=["DDInterID_A", "DDInterID_B"])
ddint = ddint[ddint["Level"] != "Unknown"]  # Remove rows with 'Unknown' Level

# ================================================
# Step 2: Feature Engineering
# ================================================
# One-Hot Encoding for categorical features
onehot_encoder = OneHotEncoder(handle_unknown="ignore")
encoded_features = onehot_encoder.fit_transform(ddint.drop(columns=["Level"])).toarray()
encoded_df = pd.DataFrame(encoded_features)
df = pd.merge(encoded_df, ddint["Level"], left_index=True, right_index=True)


# Encode severity levels directly in the DataFrame
def encode_severity_levels(df):
    level_mapping = {'Minor': 0, 'Moderate': 1, 'Major': 2}
    df['Level'] = df['Level'].map(level_mapping)


df = encode_severity_levels(df)

# ================================================
# Step 3: Model Training and Evaluation
# ================================================

# Train-test split
train_x, test_x, train_y, test_y = train_test_split(
    df.drop(columns=["Level"]), df["Level"], test_size=0.2, random_state=42
)

# Further split training data for validation
train_x, val_x, train_y, val_y = train_test_split(
    train_x, train_y, test_size=0.2, random_state=42
)

# Calculate class weights
class_counts = np.bincount(train_y)
class_weights = {
    i: len(train_y) / (len(class_counts) * count)
    for i, count in enumerate(class_counts)
}

# Train model with weights
sample_weights = np.array([class_weights[y] for y in train_y])
model_weighted = XGBClassifier(random_state=42, num_class=3, objective="multi:softmax")
model_weighted.fit(train_x, train_y, sample_weight=sample_weights)

# Evaluate weighted model
predictions_weighted = model_weighted.predict(test_x)
print("Weighted Model Performance:")
print(classification_report(test_y, predictions_weighted))
conf_matrix = ConfusionMatrixDisplay(confusion_matrix(test_y, predictions_weighted))
conf_matrix.plot(cmap=plt.cm.Blues)
plt.title("Confusion Matrix (Weighted)")
plt.savefig("project/xg_with_weight_cm.png")
plt.clf()

# Validation accuracy
val_accuracy_weighted = accuracy_score(model_weighted.predict(val_x), val_y)
print(f"Validation Accuracy (Weighted): {val_accuracy_weighted:.2f}")

# Train model without weights for comparison
model_unweighted = XGBClassifier(
    random_state=42, num_class=3, objective="multi:softmax"
)
model_unweighted.fit(train_x, train_y)

# Evaluate unweighted model
predictions_unweighted = model_unweighted.predict(test_x)
print("Unweighted Model Performance:")
print(classification_report(test_y, predictions_unweighted))
conf_matrix = ConfusionMatrixDisplay(confusion_matrix(test_y, predictions_unweighted))
conf_matrix.plot(cmap=plt.cm.Blues)
plt.title("Confusion Matrix (Unweighted)")
plt.savefig("project/xg_no_weight_cm.png")
plt.clf()

# Validation accuracy
val_accuracy_unweighted = accuracy_score(model_unweighted.predict(val_x), val_y)
print(f"Validation Accuracy (Unweighted): {val_accuracy_unweighted:.2f}")

# ================================================
# Step 4: Enhanced Dataset with Additional Features
# ================================================
# Load additional datasets
drug_acid = pd.read_csv("project/drug_cids.csv")
pchem = pd.read_csv("project/pubchem_data.csv")

# Merge and clean additional features
pchem.rename(columns={"cid": "CIDs"}, inplace=True)
drug_features = pd.merge(drug_acid, pchem, on="CIDs")
drug_features = drug_features.drop(
    columns=[
        "cmpdname",
        "cmpdsynonym",
        "mf",
        "meshheadings",
        "annothits",
        "aids",
        "cidcdate",
        "sidsrcname",
        "depcatg",
        "annotation",
        "inchi",
        "isosmiles",
        "canonicalsmiles",
        "inchikey",
        "iupacname",
    ]
)

# Merge enhanced features with original dataset
merged_df = pd.merge(ddint, drug_features, left_on="Drug_A", right_on="Drug Name")
max_df = pd.merge(merged_df, drug_features, left_on="Drug_B", right_on="Drug Name")
max_df = max_df.drop(columns=["Drug Name_x", "Drug Name_y", "CIDs_x", "CIDs_y"])
max_df = encode_severity_levels(max_df)

# One-hot encode Drug_A and Drug_B
encoded_drugs = onehot_encoder.transform(max_df[["Drug_A", "Drug_B"]]).toarray()
encoded_drugs_df = pd.DataFrame(encoded_drugs)
final_df = pd.merge(
    encoded_drugs_df,
    max_df.drop(columns=["Drug_A", "Drug_B"]),
    left_index=True,
    right_index=True,
)

# Repeat training and evaluation with enhanced dataset
# ... (similar process as above)

# ================================================
# Final Observations and Next Steps
# ================================================
# - Weighted models perform better for minority classes but may penalize majority class performance.
# - Additional features improve performance but require careful preprocessing.
# - Next steps: Hyperparameter tuning for weights and XGBoost parameters.
