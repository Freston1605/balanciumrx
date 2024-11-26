# Balancium Rx


### Project Description: Drug-Drug Interaction Prediction Dataset Preparation

This repository contains scripts, data, and processes for preparing a dataset to model drug-drug interactions (DDIs) using machine learning. The project focuses on enriching and refining an initial dataset with chemical and molecular descriptors obtained from PubChem to enable robust predictive modeling.

#### Current State of the Project

1. **Data Sources**:
   - **`ddinter_downloads_code_A.csv`**:  
     The original dataset downloaded from [DDInter](https://ddinter.scbdd.com), containing drug-drug interaction information, including interaction labels and drug names.
   - **`drug_cids.csv`**:  
     A table mapping each drug name to its respective PubChem CID (Compound Identifier). Note that some interactions involve drugs with unknown levels of interaction.
   - **`cids.csv`**:  
     A simplified version of `drug_cids.csv`, containing only the CID column. This format is optimized for batch queries on the PubChem platform.
   - **`pubchem_data.csv`**:  
     The result of querying PubChem using the `cids.csv` file. This file contains molecular descriptors and other chemical properties for each drug, identified by its CID.

2. **Next Steps**:
   - **Inspect Predictors**:  
     Examine the molecular descriptors and features available in `pubchem_data.csv` to identify relevant predictors for modeling drug-drug interactions.
   - **Create Final Dataset**:  
     Merge the selected features from `pubchem_data.csv` with the drug mappings in `drug_cids.csv` and the interaction labels in `ddinter_downloads_code_A.csv` to construct the final dataset.

#### Objectives
The ultimate goal of this project is to prepare a high-quality, feature-rich dataset that can be used to train machine learning models to predict drug-drug interactions. By combining interaction data with molecular and chemical properties, the final dataset will support accurate and reliable predictive modeling. 

This project is open to further enrichment or feature engineering based on the needs of downstream analysis.
