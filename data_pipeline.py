import os
import pandas as pd
import yaml
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-i", "--input", required=True,	type=str, default="./", help="path to input data directory")
ap.add_argument("-o", "--output", required=True, type=str, default="./", help="path to output data directory")
args = vars(ap.parse_args())


def join_pubmed_drug(input_folder):
    """Joins Pubmed data with Drugs data based on whether the drug is mentioned in the title.

    Read 2 csv to and a json pandas dataframes, clean, process and join data

    Parameters:
    input_folder : Directory of input data

    Returns: cleaned and joined pandas dataframe

    """

    drugs = pd.read_csv(os.path.join(input_folder, 'drugs.csv'))
    pubmed1 = pd.read_csv(os.path.join(input_folder, 'pubmed.csv'))
    # Please note that we are using yaml to import the json
    # because 'JSON standard does not allow trailing comma' only yaml loader allows it
    # This is not recommanded !
    with open(os.path.join(input_folder, 'pubmed.json')) as file:
        data = yaml.load(file, Loader=yaml.FullLoader)
    # concat the 2 pubmed dataframes together
    pubmed2 = pd.DataFrame(data)
    pubmed = pd.concat([pubmed1, pubmed2], ignore_index=True)
    # clean and convert date column
    pubmed['date'] = pd.to_datetime(pubmed['date']).dt.strftime("%d/%m/%Y")
    # split title column and detect whether a drug is mentioned
    pubmed['title'] = pubmed['title'].str.lower()
    pubmed['split_title'] = pubmed['title'].str.split()
    pubmed['drug'] = pubmed['split_title']. \
        apply(lambda x: list(set([item for item in x if item in drugs['drug'].str.lower().tolist()])))
    pubmed = pubmed.explode('drug')
    pubmed.drop(['split_title'], axis=1, inplace=True)
    # keep only pubmed data with detected drug
    pubmed.dropna(inplace=True)
    pubmed = pubmed.drop_duplicates(keep='last')
    # join pubmed and drugs
    drugs['drug'] = drugs['drug'].str.lower()
    merged_pubmed = pubmed.merge(drugs, how='left', on=['drug'])
    merged_pubmed['drug'] = merged_pubmed['drug'].str.upper()
    merged_pubmed.rename(columns={'id': 'id_pm', 'title': 'title_pm', 'date': 'date_pm', 'journal': 'journal_pm'},
                         inplace=True)
    return merged_pubmed


def join_clinical_trial_drugs(input_folder):
    """Joins clinical trials data with Drugs data based on whether
    the drug is mentioned in the scientific title.

    Read 2 csv to pandas dataframes, clean, process and join data

    Parameters:
    input_folder : Directory of input data

    Returns: cleaned and joined pandas dataframe

    """
    drugs = pd.read_csv(os.path.join(input_folder, 'drugs.csv'))
    clinical_trial = pd.read_csv(os.path.join(input_folder, 'clinical_trials.csv'))
    # clean and convert date column
    clinical_trial['date'] = pd.to_datetime(clinical_trial['date']).dt.strftime("%d/%m/%Y")
    # self join the data to correct missing values
    merged_ct = clinical_trial.merge(clinical_trial,how='left', on=['scientific_title'])
    merged_ct.id_x.fillna(merged_ct.id_y, inplace=True)
    merged_ct.drop(['id_y', 'date_y', 'journal_y'], axis=1, inplace=True)
    merged_ct.dropna(inplace=True)
    # split scientific title column and detect whether a drug is mentioned
    merged_ct['scientific_title'] = merged_ct['scientific_title'].str.lower()
    merged_ct['split_title'] = merged_ct['scientific_title'].str.split()
    merged_ct['drug'] = merged_ct['split_title'].apply(lambda x: list(set([item for item in x if item in drugs['drug'].str.lower().tolist()])))
    merged_ct = merged_ct.explode('drug')
    merged_ct.drop(['split_title'], axis=1, inplace=True)
    # keep only pubmed data with detected drug
    merged_ct.dropna(inplace=True)
    merged_ct = merged_ct.drop_duplicates(keep='last')
    # join clinical_trial and drugs
    drugs['drug'] = drugs['drug'].str.lower()
    merged_ct = merged_ct.merge(drugs, how='left', on=['drug'])
    merged_ct['drug'] = merged_ct['drug'].str.upper()
    merged_ct.rename(columns={'id_x': 'id_ct', 'date_x': 'date_ct', 'journal_x': 'journal_ct'}, inplace=True)
    return merged_ct


def final_join(joined_pubmed, joined_clinical_trial, output_folder):
    """Joins clinical trials data with Drugs data based on whether

    Read 2 pandas dataframes and join them based on drug

    Parameters:
    joined_pubmed (pandas dataframe): joined and cleaned pubmed data
    joined_clinical_trial (pandas dataframe): joined and cleaned clinical trial data
    output_folder : Directory to save json output

    Returns: None

    """
    final_df = pd.merge(joined_pubmed,joined_clinical_trial,how='outer', on=['drug', 'atccode'])
    final_df.to_json(os.path.join(output_folder, 'final_df.json'), orient="table")


# final_df = final_join(joined_pubmed, joined_clinical_trial)

if __name__ == "__main__":
    input_folder = args['input']
    output_folder = args['output']
    os.makedirs(output_folder, exist_ok=True)

    print("[INFO] joining pubmed and drugs data...")
    joined_pubmed = join_pubmed_drug(input_folder)
    print("[INFO] joining clinical trial and drugs data...")
    joined_clinical_trial = join_clinical_trial_drugs(input_folder)
    print("[INFO] final join...")
    final_join(joined_pubmed, joined_clinical_trial, output_folder)
