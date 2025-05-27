## LISTS
# 1. List of Squares
squares = [x**2 for x in range(1, 21)]
print("Squares:", squares)

# 2. Second Largest Number
lst = list(map(int, input("Enter list of numbers: ").split()))
first = second = float('-inf')
for num in lst:
    if num > first:
        second = first
        first = num
    elif first > num > second:
        second = num
print("Second largest number:", second)

# 3. Remove Duplicates
lst = input("Enter list elements: ").split()
seen = set()
result = []
for item in lst:
    if item not in seen:
        seen.add(item)
        result.append(item)
print("After removing duplicates:", result)

# 4. Rotate List
lst = [1, 2, 3, 4, 5]
k = 2
k %= len(lst)
rotated = lst[-k:] + lst[:-k]
print("Rotated List:", rotated)

# 5. List Compression
nums = list(map(int, input("Enter numbers: ").split()))
compressed = [x*2 for x in nums if x % 2 == 0]
print("Compressed List:", compressed)


## TUPLES
# 6. Swap Values
def swap_tuples(t1, t2):
    return t2, t1

t1 = (1, 2)
t2 = (3, 4)
t1, t2 = swap_tuples(t1, t2)
print("Swapped:", t1, t2)

# 7. Unpack Tuples
student = ("Alice", 20, "CSE", "A")
name, age, branch, grade = student
print(f"{name} is {age} years old, studies {branch}, and got grade {grade}.")

# 8. Tuple to Dictionary
tup = (("a", 1), ("b", 2))
d = dict(tup)
print("Converted Dictionary:", d)

## SETS
# 9. Common Items
list1 = input("Enter list1: ").split()
list2 = input("Enter list2: ").split()
common = set(list1) & set(list2)
print("Common Elements:", common)

# 10. Unique Words in Sentence
sentence = input("Enter a sentence: ")
words = set(sentence.split())
print("Unique Words:", words)

# 11. Symmetric Difference
set1 = set(map(int, input("Enter first set: ").split()))
set2 = set(map(int, input("Enter second set: ").split()))
sym_diff = set1 ^ set2
print("Symmetric Difference:", sym_diff)

# 12. Subset Checker
A = set(input("Enter first set: ").split())
B = set(input("Enter second set: ").split())
print("Is A subset of B?", A.issubset(B))


## DICTIONARIES
# 13. Frequency Counter
s = input("Enter a string: ")
freq = {}
for ch in s:
    freq[ch] = freq.get(ch, 0) + 1
print("Frequency:", freq)

# 14. Student Grade Book
grades = {}
for _ in range(3):
    name = input("Student name: ")
    marks = int(input("Marks: "))
    if marks >= 90:
        grade = 'A'
    elif marks >= 75:
        grade = 'B'
    else:
        grade = 'C'
    grades[name] = grade
query = input("Enter name to query grade: ")
print(f"{query}'s Grade:", grades.get(query, "Not Found"))

# 15. Merge Two Dictionaries
dict1 = {"a": 1, "b": 2}
dict2 = {"b": 3, "c": 4}
merged = dict1.copy()
for k, v in dict2.items():
    merged[k] = merged.get(k, 0) + v
print("Merged Dictionary:", merged)

# 16. Inverted Dictionary
d = {"a": 1, "b": 2}
inverted = {v: k for k, v in d.items()}
print("Inverted Dictionary:", inverted)

# 17. Group Words by Length
words = input("Enter words: ").split()
length_dict = {}
for word in words:
    length_dict.setdefault(len(word), []).append(word)
print("Grouped by Length:", length_dict)
