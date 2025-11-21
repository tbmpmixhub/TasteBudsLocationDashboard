# This file is used to test the SFTP connection to Toast servers
import paramiko
import os
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("SFTP_HOST")
USERNAME = os.getenv("SFTP_USERNAME")
KEY_PATH = os.getenv("SFTP_KEY_PATH") # file exported from PuTTYgen in the project root

def test_sftp():
    try:
        print("Loading key...")
        key = paramiko.RSAKey.from_private_key_file(KEY_PATH)

        print("Connecting to SFTP...")
        transport = paramiko.Transport((HOST, 22))
        transport.connect(username=USERNAME, pkey=key)

        sftp = paramiko.SFTPClient.from_transport(transport)

        print("✅ Connected! Listing top-level directory:")
        for name in sftp.listdir("."):
            print(" -", name)

        sftp.close()
        transport.close()
        print("✅ SFTP connection closed cleanly.")
    except Exception as e:
        print("❌ SFTP error:", repr(e))

if __name__ == "__main__":
    test_sftp()
