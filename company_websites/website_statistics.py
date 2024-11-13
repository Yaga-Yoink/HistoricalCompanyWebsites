import pandas as pd
import glob
import re
import matplotlib.pyplot as plt

class Website_CSV_Statistics:
    def __init__(self, dir_path):
        self.dir_path = dir_path

    # Returns a dataframe which combines all CSV files in 'dir_path' along the common columns names. Requires: CSV files have the same column names. Raises: Exception 
    def load_csv(self):
        # ** recursively goes through every subdirectory
        # * matches any sequence of characters within the directory level
        files = glob.glob(f"{self.dir_path}**/*.csv", recursive=True)
        print(files)
        if len(files) == 1: 
            return pd.read_csv(files[0], index_col="CompanyName")
        elif len(files) > 1:
            dfs = map(lambda file : pd.read_csv(file, index_col="CompanyName"), files)
            result_df = pd.concat(dfs)
            result_df = result_df.loc[~result_df.index.duplicated(keep="first")]
            return result_df
        else:
            raise Exception("load_csv requires there to be a matching CSV in the dir_path")

    # Returns a dictionary {year i: number of websites collected on year i, ... year n : number of websites collected on year n}. Requires: text file names end with yyyymmddhhmmss.txt
    def year_count(self, df: pd.DataFrame):
        # Appends the year as a key to year_dict and increments the value.
        def add_year(cell):
            # Possibly figure out a better way to match the year
            year = cell[-18:-14]
            if year in year_dict.keys():
                year_dict[year] += 1
            else:
                year_dict[year] = 1
        year_dict = {}
        df = df.filter(axis=1, regex="text_version_[0-9]+$")
        df = df.fillna('')
        df.map(add_year)
        # Remove the non-existant years used for filling nan values
        if '' in year_dict.keys():
            year_dict.pop('')
        return year_dict
    
    #Returns a dictionary {year i: number of unique company websites collected on year i, ... year n : number of unique company websites collected on year n}
    def unique_per_year(self, df: pd.DataFrame):
              # Appends the year as a key to year_dict and increments the value.
        def add_year(cell):
            # Name of the company
            # ^ is negation, [^/] matches any non-slash character, [^/]+ matches 1 or more non-slash character 
            name_match = re.search("/website_text/[^/]+/([^/]+)/[^/]+\.txt", cell)
            if name_match:
                name = name_match.group(1)
                year = cell[-18:-14]
                if year in unique_year.keys():
                    if name not in unique_year[year][1]:
                        unique_year[year] = (unique_year[year][0] + 1, unique_year[year][1] + [name])
                else:
                    unique_year[year] = (1, [name])
        # {year : (num of unique companies that year, [name of companies])}
        unique_year = {}
        df = df.filter(axis=1, regex="text_version_[0-9]+$")
        df = df.fillna("")
        df.map(add_year)
        # Remove the extra information about which companies were documented in that year
        return {key : unique_year[key][0] for key in unique_year.keys()}
    
    # Returns the average length of the text for all the text files in 'self.dir_path'.
    def average_text_length(self):
        files = glob.glob(f"{self.dir_path}**/*.txt", recursive=True)
        text_length = 0
        for file in files:
            text_length += len(open(file).read())
        return text_length / len(files)
    
    #TODO: add parameters for custom title and label
    # Plot a bar chart in ascending label order where the keys of dict are x labels and values are y labels. 
    def plot_year(self, year_dict : dict):
        fig, ax = plt.subplots()
        plt.xticks(fontsize = "xx-small")
        year_dict = dict(sorted(year_dict.items()))
        ax.bar(year_dict.keys(), year_dict.values())
        plt.show()
        pass
        

# TODO: make some pretty graphs and analytics pictures for the calculated stats


if __name__ == "__main__":
    website_stats = Website_CSV_Statistics("final_g2_run/")
    df = website_stats.load_csv()
    print("df : ", df.head(2))
    year_count_dict = website_stats.year_count(df)
    print("Year count dict : ", year_count_dict)
    unique_per_year_dict = website_stats.unique_per_year(df)
    print("Unique per year dict :", unique_per_year_dict)
    # average_text_length = website_stats.average_text_length()
    # print(f"Average text length (chars): {average_text_length}")
    website_stats.plot_year(year_count_dict)
    
