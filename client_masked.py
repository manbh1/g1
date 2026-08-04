import os
import sys
import requests

# --- CONFIGURATION ---
SERVER_URL = ""
SECRET_ROUTE = ""
SECRET_TOKEN = ""

def upload_file(file_path: str):
    if not os.path.isfile(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        return

    file_name = os.path.basename(file_path)
    full_target_url = f"{SERVER_URL.rstrip('/')}{SECRET_ROUTE}"

    headers = {
        "X-Secret-Token": SECRET_TOKEN,
        "X-File-Name": file_name,
        "Content-Type": "application/octet-stream"
    }

    print(f"Uploading '{file_name}' to server...")

    # Pass the opened file object directly to stream data in binary mode
    with open(file_path, "rb") as file_data:
        try:
            response = requests.post(
                full_target_url,
                headers=headers,
                data=file_data
            )

            if response.status_code == 200:
                print("Upload Successful!")
                print("Server response:", response.json())
            elif response.status_code == 404:
                print("Error: 404 Not Found (Invalid route or token).")
            else:
                print(f"Failed with HTTP Status {response.status_code}: {response.text}")

        except Exception as e:
            print(f"Connection error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python client.py <path_to_file>")
    else:
        upload_file(sys.argv[1])