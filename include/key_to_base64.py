import base64

def convert(key_path):
    with open(key_path, "rb") as key_file:
        private_key_content = base64.b64encode(key_file.read()).decode("utf-8")
        print(private_key_content)

if __name__ == "__main__":
    convert(key_path="path/to/private_key.p8")
