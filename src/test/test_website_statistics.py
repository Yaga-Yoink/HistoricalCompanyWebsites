import os, sys
# To get the parent directory which has website_statistics
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from website_statistics import Website_CSV_Statistics
import os
import unittest

class TestWebsiteStats(unittest.TestCase):
    
    def setUp(self):
        self.test_folder_path = "src/test/website_statistics_test_folder/"
        self.historical_versions_test_path = os.path.join(self.test_folder_path, "historical_versions")
        self.WebsiteStatistics = Website_CSV_Statistics(self.test_folder_path)
        
        self.test_df = self.WebsiteStatistics.load_csv()

    def test_load_csv(self):
        self.assertEqual(list(self.WebsiteStatistics.load_csv().columns), ["URL","probability","Description","text_version_1","text_version_1_similarity_score","text_version_2","text_version_2_similarity_score"])
    
    def test_average_text_length(self):
        self.assertEqual(self.WebsiteStatistics.average_text_length(), 16.0)
    
    def test_empty_text_file_paths(self):
        # There is already an empty text file in the directory, so there will always be atleast one. 
        self.assertGreater(len(self.WebsiteStatistics.empty_text_file_paths()), 0)
        
    def test_year_count(self):
        self.assertEqual(self.WebsiteStatistics.year_count(self.test_df), {'2020' : 6, '2023' : 2})
    
    def test_unique_per_year(self):
        self.assertEqual(self.WebsiteStatistics.unique_per_year(self.test_df), {'2020' : 4, '2023' : 2})
        
    def test_num_empty_files(self):
        self.assertEqual(self.WebsiteStatistics.num_empty_files(), 1)
        
if __name__ == "__main__":
    unittest.main()
        
