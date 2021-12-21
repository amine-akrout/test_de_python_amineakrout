import os
import pandas as pd

output_folder = "./output"


def top_journal(output_folder):
    df = pd.read_json(os.path.join(output_folder, 'final_df.json'), orient="table")
    df = df[['drug', 'journal_pm']]
    df.dropna(inplace=True)
    df = df.drop_duplicates(keep='last')

    df_top = df.groupby(['journal_pm'], as_index=False).drug.count().sort_values(by='drug', ascending=False).reset_index(drop=True)
    top_jr = df_top['journal_pm'].loc[df_top['drug'] == df_top['drug'].max()].reset_index(drop=True)

    if top_jr.size == 1:
        return print("le journal qui mentionne le plus de médicaments différents est: {}".format(top_jr[0]))
    else:
        text = ""
        for i in range(top_jr.size):
            text += "'" + top_jr[i] + "' ,"
        return print("les journaux qui mentionnent le plus de médicaments différents sont: {}".format(text))


top_journal("./output")
