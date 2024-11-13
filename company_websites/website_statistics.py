import pandas as pd
import glob
import re
import matplotlib.pyplot as plt

import nltk
nltk.download('popular')
nltk.download('punkt')
nltk.download('punkt_tab')

class Website_CSV_Statistics:
    def __init__(self, dir_path):
        self.dir_path = dir_path

    # Return a list of all the paths to the text files within all subdirectories of 'self.dir_path'
    def text_files(self):
        return glob.glob(f"{self.dir_path}**/*.txt", recursive=True)

    # Returns a dataframe which combines all CSV files in 'dir_path' along the common columns names. Requires: CSV files have the same column names. Raises: Exception
    def load_csv(self):
        # ** recursively goes through every subdirectory
        # * matches any sequence of characters within the directory level
        files = glob.glob(f"{self.dir_path}**/*.csv", recursive=True)
        if len(files) == 1:
            return pd.read_csv(files[0], index_col="CompanyName")
        elif len(files) > 1:
            dfs = map(lambda file: pd.read_csv(file, index_col="CompanyName"), files)
            result_df = pd.concat(dfs)
            result_df = result_df.loc[~result_df.index.duplicated(keep="first")]
            return result_df
        else:
            raise Exception(
                "load_csv requires there to be a matching CSV in the dir_path"
            )

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
        df = df.fillna("")
        df.map(add_year)
        # Remove the non-existant years used for filling nan values
        if "" in year_dict.keys():
            year_dict.pop("")
        return year_dict

    # Returns a dictionary {year i: number of unique company websites collected on year i, ... year n : number of unique company websites collected on year n}
    def unique_per_year(self, df: pd.DataFrame):
        # Appends the year as a key to year_dict and increments the value.
        def add_year(cell):
            # Name of the company
            # ^ is negation, [^/] matches any non-slash character, [^/]+ matches 1 or more non-slash character
            name_match = re.search(r"/website_text/[^/]+/([^/]+)/[^/]+\.txt", cell)
            if name_match:
                name = name_match.group(1)
                year = cell[-18:-14]
                if year in unique_year.keys():
                    if name not in unique_year[year][1]:
                        unique_year[year] = (
                            unique_year[year][0] + 1,
                            unique_year[year][1] + [name],
                        )
                else:
                    unique_year[year] = (1, [name])

        # {year : (num of unique companies that year, [name of companies])}
        unique_year = {}
        df = df.filter(axis=1, regex="text_version_[0-9]+$")
        df = df.fillna("")
        df.map(add_year)
        # Remove the extra information about which companies were documented in that year
        return {key: unique_year[key][0] for key in unique_year.keys()}

    # Returns the average length of the text for all the text files in 'self.dir_path' that are not empty.
    def average_text_length(self):
        empty_files = self.empty_text_file_paths()
        files = list(filter(lambda x : x not in empty_files , self.text_files()))
        text_length = 0
        for file in files:
            with open(file) as opened_file:
                text_length += len(opened_file.read())
        return text_length / len(files)

    # Returns a list of all the filepaths in 'self.dir_path' that are empty. 
    def empty_text_file_paths(self):
        files = self.text_files()
        result = []
        for file in files:
            with open(file) as opened_file:
                if len(opened_file.read()) == 0:
                    result.append(file)
        return result
    
    # Returns the number of text files in 'self.dir_path' that are empty.
    def num_empty_files(self):
        files = self.text_files()
        return len(self.empty_text_file_paths())
    
    # Returns the number of text files in 'self.dir_path'.
    def total_num_files(self):
        files = self.text_files()
        return len(files)

    # Plot a bar chart in ascending label order where the keys of dict are x labels and values are y labels. 'graph_info' contains [title, xlabel, ylabel]
    def plot_year(self, year_dict: dict, graph_info: list):
        fig, ax = plt.subplots()
        plt.xticks(fontsize="xx-small")
        year_dict = dict(sorted(year_dict.items()))
        ax.bar(year_dict.keys(), year_dict.values())
        ax.set_title(graph_info[0])
        ax.set_xlabel(graph_info[1])
        ax.set_ylabel(graph_info[2])
        plt.show()

    # Return a dictionary with the {word : number of occurences in all text files} from all of the text files in 'self.dir_path' where only non-stopword words are included.
    def word_frequency(self):
        stop_words = set(nltk.corpus.stopwords.words('English'))
        files = self.text_files()
        dict = {}
        for file in files:
            with open(file) as opened_file:
                file_text = opened_file.read()
                # file_text = nltk.word_tokenize(file_text)
                file_text = file_text.lower().split(" ")
                for word in file_text:
                    if word not in stop_words:
                        if word in dict.keys():
                            dict[word] += 1
                        else:
                            dict[word] = 1
        return dict


# TODO: make some pretty graphs and analytics pictures for the calculated stats


if __name__ == "__main__":
    website_stats = Website_CSV_Statistics("final_g2_run/")
    df = website_stats.load_csv()
    print("df : ", df.head(2))
    year_count_dict = website_stats.year_count(df)
    print("Year count dict : ", year_count_dict)
    unique_per_year_dict = website_stats.unique_per_year(df)
    print("Unique per year dict :", unique_per_year_dict)
    average_text_length = website_stats.average_text_length()
    print(f"Average text length (chars): {average_text_length}")
    # website_stats.plot_year(
    #     year_count_dict,
    #     [
    #         "Number of Company Websites Collected Per Year",
    #         "Year",
    #         "Number of Company Websites",
    #     ],
    # )
    # website_stats.plot_year(
    #     unique_per_year_dict,
    #     [
    #         "Number of Unique Companies Collected Per Year",
    #         "Year",
    #         "Number of Unique Companies",
    #     ],
    # )
    
    
    num_empty = website_stats.num_empty_files()
    print("Number of Empty Website Text Files: ", num_empty)
    word_freq = website_stats.word_frequency()
    print("Word frequency : ",  dict(sorted(word_freq.items(), key = lambda x : x[1])))
    num_files = website_stats.total_num_files()
    print("Number of Non-empty Text Files: ", num_files - num_empty)
    
