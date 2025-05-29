# FUNCTIONS
# 1. Prime Number Checker
def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(n**0.5)+1):
        if n % i == 0:
            return False
    return True

print("Prime numbers between 1 and 100:")
for i in range(1, 101):
    if is_prime(i):
        print(i, end=" ")
print("\n")

# 2. Temperature Converter
def convert_temp(value, unit):
    if unit == "C":
        return (value * 9/5) + 32
    elif unit == "F":
        return (value - 32) * 5/9
    else:
        return "Invalid unit"

print("25°C to Fahrenheit:", convert_temp(25, "C"))
print("77°F to Celsius:", convert_temp(77, "F"))
print()

# 3. Recursive Factorial Function
def factorial(n):
    if n == 0 or n == 1:
        return 1
    return n * factorial(n - 1)

print("Factorial of 5:", factorial(5))
print()

##############################
# CLASSES
# 4. Rectangle Class
class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width

    def area(self):
        return self.length * self.width

    def perimeter(self):
        return 2 * (self.length + self.width)

    def is_square(self):
        return self.length == self.width

rect = Rectangle(10, 10)
print("Rectangle area:", rect.area())
print("Rectangle perimeter:", rect.perimeter())
print("Is square:", rect.is_square())
print()

# 5. BankAccount Class
class BankAccount:
    def __init__(self, name, balance=0):
        self.name = name
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount

    def withdraw(self, amount):
        if amount > self.balance:
            print("Insufficient balance.")
        else:
            self.balance -= amount

    def get_balance(self):
        return self.balance

acc = BankAccount("John", 1000)
acc.deposit(500)
acc.withdraw(300)
print("Balance:", acc.get_balance())
acc.withdraw(1500)
print()

# 6. Book Class
class Book:
    def __init__(self, title, author, price, in_stock):
        self.title = title
        self.author = author
        self.price = price
        self.in_stock = in_stock

    def sell(self, quantity):
        if quantity > self.in_stock:
            raise ValueError("Not enough stock!")
        self.in_stock -= quantity

book = Book("Python 101", "John Doe", 29.99, 10)
try:
    book.sell(3)
    print("Books left:", book.in_stock)
except ValueError as e:
    print(e)
print()

# 7. Student Grade System
class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def average(self):
        return sum(self.marks) / len(self.marks)

    def grade(self):
        avg = self.average()
        if avg >= 90:
            return "A"
        elif avg >= 75:
            return "B"
        elif avg >= 50:
            return "C"
        else:
            return "F"

student = Student("Alice", [85, 90, 78])
print(f"{student.name}'s Average: {student.average()}")
print(f"{student.name}'s Grade: {student.grade()}")
print()
#####################################
# INHERITANCE
# 8. Person → Employee
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

class Employee(Person):
    def __init__(self, name, age, emp_id, salary):
        super().__init__(name, age)
        self.emp_id = emp_id
        self.salary = salary

    def display_info(self):
        print(f"Name: {self.name}, Age: {self.age}, ID: {self.emp_id}, Salary: {self.salary}")

emp = Employee("Bob", 30, "E123", 50000)
emp.display_info()
print()

# 9. Vehicle → Car, Bike
class Vehicle:
    def __init__(self, name, wheels):
        self.name = name
        self.wheels = wheels

    def description(self):
        return f"{self.name} has {self.wheels} wheels."

class Car(Vehicle):
    def __init__(self, name, wheels, fuel_type):
        super().__init__(name, wheels)
        self.fuel_type = fuel_type

    def description(self):
        return f"{self.name} is a car with {self.wheels} wheels and runs on {self.fuel_type}."

class Bike(Vehicle):
    def __init__(self, name, wheels, is_geared):
        super().__init__(name, wheels)
        self.is_geared = is_geared

    def description(self):
        gear_status = "geared" if self.is_geared else "non-geared"
        return f"{self.name} is a {gear_status} bike with {self.wheels} wheels."

car = Car("Toyota", 4, "Petrol")
bike = Bike("Yamaha", 2, True)
print(car.description())
print(bike.description())
print()

# 10. Polymorphism with Animals
class Animal:
    def speak(self):
        pass

class Dog(Animal):
    def speak(self):
        return "Woof!"

class Cat(Animal):
    def speak(self):
        return "Meow!"

class Cow(Animal):
    def speak(self):
        return "Moo!"

animals = [Dog(), Cat(), Cow()]
for animal in animals:
    print(animal.speak())
