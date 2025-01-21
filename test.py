user_num = int(input("Please enter an int: "))
x = int(input("How many times to divide: "))
output = []

for i in range(4):
    user_num = user_num / x
    output.append(str(user_num))
print(" ".join(output))