
## VARIABLES, DATA TYPES, OPERATORS
# 1. Digit Sum Calculator
num = input("Enter a number: ")
dig_sum = sum(int(d) for d in num)
print("Digit Sum:", dig_sum)

# 2. Reverse a 3-digit Number
num = input("Enter a 3-digit number: ")
if len(num) == 3 and num.isdigit():
    print("Reversed:", num[::-1])
else:
    print("Invalid 3-digit number.")

#3. Unit Converter
meters = float(input("Enter distance in meters: "))
print("Centimeters:", meters * 100)
print("Feet:", meters * 3.28084)
print("Inches:", meters * 39.3701)

#4. Percentage Calculator
marks = [float(input(f"Enter marks for subject {i+1}: ")) for i in range(5)]
total = sum(marks)
average = total / 5
percentage = (total / 500) * 100
print(f"Total: {total}, Average: {average}, Percentage: {percentage:.2f}%")
if percentage >= 90:
    grade = "A"
elif percentage >= 75:
    grade = "B"
elif percentage >= 60:
    grade = "C"
elif percentage >= 40:
    grade = "D"
else:
    grade = "F"
print("Grade:", grade)

##  CONDITIONALS
# 5. Leap Year Checker
year = int(input("Enter a year: "))
if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
    print("Leap year")
else:
    print("Not a leap year")

# 6. Simple Calculator
a = float(input("Enter first number: "))
b = float(input("Enter second number: "))
op = input("Enter operator: ")
if op == '+':
    print("Result:", a + b)
elif op == '-':
    print("Result:", a - b)
elif op == '*':
    print("Result:", a * b)
elif op == '/':
    print("Result:", a / b if b != 0 else "Cannot divide by zero")
else:
    print("Invalid operator")

# 7. Triangle Validator
a, b, c = map(int, input("Enter 3 side lengths: ").split())
if a + b > c and b + c > a and a + c > b:
    print("Valid triangle")
else:
    print("Invalid triangle")

# 8. Bill Splitter with Tip
total = float(input("Enter total bill amount: "))
people = int(input("Enter number of people: "))
tip_percent = float(input("Enter tip percentage: "))
tip_amount = total * (tip_percent / 100)
total_with_tip = total + tip_amount
per_person = total_with_tip / people
print(f"Each person should pay: {per_person:.2f}")

##  LOOPS
# 9. Find All Prime Numbers Between 1 and 100
print("Prime numbers between 1 and 100:")
for num in range(2, 101):
    is_prime = True
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            is_prime = False
            break
    if is_prime:
        print(num, end=' ')
print()

# 10. Palindrome Checker
text = input("Enter a string: ")
if text == text[::-1]:
    print("Palindrome")
else:
    print("Not a palindrome")

# 11. Fibonacci Series (First N Terms)
n = int(input("Enter number of terms: "))
a, b = 0, 1
for _ in range(n):
    print(a, end=' ')
    a, b = b, a + b
print()

# 12. Multiplication Table (User Input)
num = int(input("Enter a number: "))
for i in range(1, 11):
    print(f"{num} x {i} = {num * i}")

# 13. Number Guessing Game
import random
secret = random.randint(1, 100)
while True:
    guess = int(input("Guess the number (1-100): "))
    if guess < secret:
        print("Too Low!")
    elif guess > secret:
        print("Too High!")
    else:
        print("Correct! The number was", secret)
        break

# 14. ATM Machine Simulation
balance = 10000
while True:
    print("\nATM Menu:\n1. Deposit\n2. Withdraw\n3. Check Balance\n4. Exit")
    choice = input("Choose an option: ")
    if choice == '1':
        amt = float(input("Enter amount to deposit: "))
        balance += amt
        print("Deposited.")
    elif choice == '2':
        amt = float(input("Enter amount to withdraw: "))
        if amt <= balance:
            balance -= amt
            print("Withdrawn.")
        else:
            print("Insufficient balance.")
    elif choice == '3':
        print("Current balance:", balance)
    elif choice == '4':
        print("Thank you for using ATM.")
        break
    else:
        print("Invalid option")

# 15. Password Strength Checker
import re
password = input("Enter a password: ")
if (len(password) >= 8 and
    re.search(r'[A-Z]', password) and
    re.search(r'[a-z]', password) and
    re.search(r'\d', password) and
    re.search(r'[!@#$%^&*(),.?":{}|<>]', password)):
    print("Strong password")
else:
    print("Weak password")

# 16. Find GCD (Greatest Common Divisor)
a = int(input("Enter first number: "))
b = int(input("Enter second number: "))
while b:
    a, b = b, a % b
print("GCD is:", a)
