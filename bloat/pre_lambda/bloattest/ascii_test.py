# from transformers import AutoTokenizer

# tokenizer = AutoTokenizer.from_pretrained("sshleifer/distilbart-cnn-12-6")
# text = 'hello how are you?'

# tokens = tokenizer(text, return_offsets_mapping=True, truncation=False)
# input_ids = tokens["input_ids"]
# print(tokens)
# print(input_ids)

# new_text = tokenizer.decode(input_ids).lstrip('<s>').rstrip('</s>')
# print(new_text)
# tokens = tokenizer(new_text, return_offsets_mapping=True, truncation=False)
# input_ids = tokens["input_ids"]
# print(input_ids)

# from datetime import datetime

# # Your timestamp string
# timestamp_str = "2024-09-02T06:03:10Z"

# # Parse the string into a datetime object
# dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
# unix_time = int(dt.timestamp())

# print(unix_time)

#print("measure at the least.â€".encode("utf-8"))

def remove_non_ascii_encode_decode(text):
    return text.encode('ascii', 'ignore').decode('ascii')

my_string = "Hello, Wörld! This is a test string with some unicode characters: éàç"
clean_string = remove_non_ascii_encode_decode(my_string)
print(clean_string)