import os, sys
# To get the parent directory which has website_statistics
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from website_statistics import Website_CSV_Statistics
import unittest
import pandas as pd

class TestWebsiteStats(unittest.TestCase):
    
    def setUp(self):
        self.test_folder_path = "company_websites/testing/website_statistics_test_folder"
        self.historical_versions_test_path = os.path.join(self.test_folder_path, "historical_versions")
        self.website_statistics = Website_CSV_Statistics(self.historical_versions_test_path)
        
        self.test_df = self.website_statistics.load_csv()

    def test_load_csv(self):
        self.assertEqual(list(self.website_statistics.load_csv().columns), ["URL","probability","Description","text_version_1","text_version_1_similarity_score","text_version_2","text_version_2_similarity_score"])
        
    def test_year_count(self):
        self.assertEqual(self.website_statistics.year_count(self.test_df), {'2020' : 6, '2023' : 2})
    
    def test_unique_per_year(self):
        self.assertEqual(self.website_statistics.unique_per_year(self.test_df), {'2020' : 4, '2023' : 2})
        
if __name__ == "__main__":
    unittest.main()
        
