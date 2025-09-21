import os
import glob
import pandas as pd
import time

class LocalFileService:
    def retrieve_recent_data_files(self, file_pattern, date_column=None, date_value=None):
        """ 
        Dynamically extract data from CSV or JSON files matching the pattern.
        Optionally filter by date_column and date_value.
        Returns a concatenated DataFrame of all matching rows.
        """
        files = glob.glob(f".{file_pattern}")
        now = time.time()
        last_24_hours = now - 24 * 60 * 60
        # recent_files = [f for f in files if os.path.getmtime(f) >= last_24_hours]
        
        # temp - all files
        # TODO - restore last_24_hours filter
        recent_files = files
        data_frames = []
        for file in recent_files:
            if file.endswith('.csv'):
                df = pd.read_csv(file)
            elif file.endswith('.json'):
                df = pd.read_json(file)
            else:
                continue
            if date_column and date_value and date_column in df.columns:
                df = df[df[date_column] == date_value]
            data_frames.append(df)
        if data_frames:
            return pd.concat(data_frames, ignore_index=True)
        else:
            raise FileNotFoundError(f"No files found in last 24 hours for pattern: {file_pattern}")
        

    def retrieve_last_saved_file(self, file_pattern, number_of_rows=4):
        """
        Retrieve the last file saved and return the last N rows.
        """
        files = glob.glob(file_pattern)
        if not files:
            raise FileNotFoundError(f"No files found for pattern: {file_pattern}")
        latest_file = max(files, key=os.path.getmtime)
        if latest_file.endswith('.csv'):
            df = pd.read_csv(latest_file)
        elif latest_file.endswith('.json'):
            df = pd.read_json(latest_file)
        else:
            raise ValueError("Unsupported file format. Only .csv and .json are supported.")
        return df.tail(number_of_rows)  
    
    def check_file_exists(self, file_path):
        """
        Check if a file exists at the given path.
        """
        return glob.glob(file_path) != []
