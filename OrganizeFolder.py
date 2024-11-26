import os
import shutil
from datetime import datetime

def organize_folder(folder, include_subfolders=False, log_file="organize_log.txt"):
    """
    Organizes files in the given folder into categorized subfolders.

    Args:
        folder (str): Path to the folder to organize.
        include_subfolders (bool): Whether to include subfolders in the organization process.
        log_file (str): Path to the log file for recording actions.
    """
    file_types = {
        'Images': ['.jpeg', '.jpg', '.png', '.gif', '.bmp', '.tiff'],
        'Videos': ['.mp4', '.avi', '.mov', '.mkv'],
        'Documents': ['.pdf', '.docx', '.doc', '.txt', '.xls', '.xlsx', '.ppt', '.pptx'],
        'Archives': ['.zip', '.rar', '.tar', '.gz', '.7z'],
        'Audio': ['.mp3', '.wav', '.aac', '.flac']
    }

    uncategorized_folder = os.path.join(folder, "Uncategorized")
    os.makedirs(uncategorized_folder, exist_ok=True)

    log_lines = []
    log_lines.append(f"--- Organization started: {datetime.now()} ---")

    if include_subfolders:
        file_paths = [os.path.join(root, f) for root, _, files in os.walk(folder) for f in files]
    else:
        file_paths = [os.path.join(folder, f) for f in os.listdir(folder)]

    for file_path in file_paths:
        if os.path.isfile(file_path): 
            filename = os.path.basename(file_path)
            ext = os.path.splitext(filename)[1].lower()  

            moved = False
            for folder_name, extensions in file_types.items():
                if ext in extensions:
                    target_folder = os.path.join(folder, folder_name)
                    os.makedirs(target_folder, exist_ok=True)
                    shutil.move(file_path, os.path.join(target_folder, filename)) 
                    log_lines.append(f"Moved {filename} to {folder_name}")
                    print(f"Moved {filename} to {folder_name}")
                    moved = True
                    break

            if not moved:  
                shutil.move(file_path, os.path.join(uncategorized_folder, filename))
                log_lines.append(f"Moved {filename} to Uncategorized")
                print(f"Moved {filename} to Uncategorized")

    log_lines.append(f"--- Organization finished: {datetime.now()} ---\n")

    with open(log_file, "a") as log:
        log.write("\n".join(log_lines) + "\n")
    print(f"Organization complete! Logs saved to {log_file}")

organize_folder(r'C:\Users\SD-LORENZO-PC\Downloads', include_subfolders=True)
