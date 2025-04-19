import os

directory = r'C:\Users\tucke\OneDrive\Documents\School\IS 3700\Final Project\images\portfolio'

files = os.listdir(directory)

num = 0 

for i in files:
    current_num = int(i.split('_')[1].split('.')[0])

    if current_num > num:
        num = current_num
    else:
        pass

for file in files:
    old_file_path = os.path.join(directory, file)

    if os.path.isfile(old_file_path):
        file_extension = os.path.splitext(file)[1]

        new_file_name = f"image_{num}{file_extension}"
        new_file_path = os.path.join(directory, new_file_name)

        os.rename(old_file_path, new_file_path)
        print(f"Renamed: {old_file_path} -> {new_file_path}")

        num += 1

for i in os.listdir(directory):
    print(f"'{i}',")



