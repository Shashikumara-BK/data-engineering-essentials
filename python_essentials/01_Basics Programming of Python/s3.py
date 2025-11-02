# import boto3

# # Create an S3 client
# s3 = boto3.client('s3')

# # List buckets
# response = s3.list_buckets()

# print("S3 Buckets:")
# for bucket in response['Buckets']:
#     print(f"  - {bucket['Name']}")



# Input list
# input_list = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 6]

# # Step 1: Count frequencies manually
# freq_dict = {}
# for num in input_list:
#     if num in freq_dict:
#         freq_dict[num] += 1
#     else:
#         freq_dict[num] = 1

# # Step 2: Sort by frequency (descending)
# sorted_freq = sorted(freq_dict.items(), key=lambda item: item[1], reverse=True)

# # Step 3: Get top 3
# top_3 = sorted_freq[:3]

# print(top_3)



from collections import Counter

# Input list
input_list = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 6]

# Count frequencies
counter = Counter(input_list)

# Get the top 3 most common elements
top_3 = counter.most_common(2)

print(top_3)
