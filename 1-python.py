# Databricks notebook source
# MAGIC %md # Notebook 1: Python Overview
# MAGIC 
# MAGIC ## Motivations
# MAGIC 
# MAGIC Spark provides multiple *Application Programming Interfaces* (API), i.e. the interface allowing the user to interact with the application. The main APIs are [Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package) and [Java](http://spark.apache.org/docs/latest/api/java/index.html) APIs, as Spark is implemented in Scala and runs on the Java Virtual Machine (JVM).
# MAGIC Since the 0.7.0 version, a [Python API](http://spark.apache.org/docs/latest/api/python/index.html) is available, also known as PySpark. An [R API](http://spark.apache.org/docs/latest/api/R/index.html) has been released with 1.5.0 version. During this course, you will be using Spark 2.2.
# MAGIC 
# MAGIC Throughout this course we will use the Python API for the following reasons:
# MAGIC - R API is still too young and limited to be relied on. Besides, R can quickly become a living hell when using immature libraries.
# MAGIC - Many of you are wanabee datascientists, and Python is a must-know language in data industry.
# MAGIC - Scala and Java APIs would have been quite hard to learn given the length of the course and your actual programming skills.
# MAGIC - Python is easy to learn, and even easier if you are already familiar with R.
# MAGIC 
# MAGIC The goal of this session is to teach (or remind) you the syntax of basic operations, control structures and declarations in Python that will be useful when using Spark. Keep in mind that we do not have a lot of time, and that you should be able to create functions and classes and to manipulate them at the end of the lab. If you don't get that, the rest of the course will be hard to follow. Don't hesitate to ask for explanations and/or more exercises if you don't feel  confident enough at the end of the lab.
# MAGIC 
# MAGIC **Note:** Python comes in two flavours, Python 2 and Python 3. For some reasons, the two versions coexist today. However, Python 2 is reaching end of life and will stop being maintained in 2020. However, the default python version used in many production systems and companies is Python 2. That is why this course will use Python 2 rather than Python 3. Note that if you know Python 2, learning Python 3 will be lightning-fast.
# MAGIC For those who are interested, you can quickly learn Python 3 syntax [over here](https://learnxinyminutes.com/docs/python3/) or find some [cheatsheets](http://ptgmedia.pearsoncmg.com/imprint_downloads/informit/promotions/python/python2python3.pdf) highlighting the differences between the two versions. Spark Python API is compatible with Python 3 since Spark 1.4.0.
# MAGIC 
# MAGIC *This introduction relies on [Learn Python in Y minutes](https://learnxinyminutes.com/docs/python/)*
# MAGIC 
# MAGIC ## Introduction
# MAGIC 
# MAGIC Python is a high level, general-purpose interpreted language. Python is meant to be very concise and readable, it is thus a very pleasant language to work with. 
# MAGIC 
# MAGIC ## 1. Primitive Datatypes and Operators
# MAGIC Read section 1 of [Learn python in Y Minutes](https://learnxinyminutes.com/docs/python/) (if you already know Python, you can skip this step). Then, replace `???` in the following cells with your code to answer the questions. To get started, please run the following cell.

# COMMAND ----------

# Run this cell, it loads a Test object
# that will allow you to check your code
# for some questions.
from test_helper import Test

# COMMAND ----------

# MAGIC %md Compute 4 + 8

# COMMAND ----------

4+8

# COMMAND ----------

# MAGIC %md Compute 4 * 8

# COMMAND ----------

4*8

# COMMAND ----------

# MAGIC %md Compute 4 / 8 (using the regular division operation, not integer division)

# COMMAND ----------

4./8

# COMMAND ----------

# MAGIC %md Why division can be tricky in Python 2?

# COMMAND ----------

# Because one need to be careful to which objects he uses it, 4 must be be converted to a float.

# COMMAND ----------

# MAGIC %md Check if the variable `foo` is None:

# COMMAND ----------

foo = None
foo is None  # => True

# COMMAND ----------

# MAGIC %md ## 2. Variables and Collections
# MAGIC Same as before, read the corresponding section, and answer the questions below.

# COMMAND ----------

# Declare a variable containing a float of your choice and print it
# From now on, when you will be asked to print something, please use the print statement.
x = 3.0
print(x)
type(x)

# COMMAND ----------

# Create a list containing strings and store it in a variable
bar = ['Havana','Drink']
# Append a new string to this list
bar.append('Club')
# Append an integer to this list and print it
bar.append(3)
print(bar)

# COMMAND ----------

# MAGIC %md Note that the modifications on list objects are performed inplace, i.e.
# MAGIC 
# MAGIC     li = [1, 2, 3]
# MAGIC     li.append(4)
# MAGIC     li  # => [1, 2, 3, 4]

# COMMAND ----------

# Mixing types inside a list object can be a bad idea depending on the situation.
# Remove the integer you just inserted in the list and print it
bar.pop()
print(bar)

# COMMAND ----------

# Print the second element of the list
print(bar[1])

# COMMAND ----------

# MAGIC %md You can access list elements in reverse order, e.g.
# MAGIC 
# MAGIC     li[-1]  # returns the last element of the list
# MAGIC     li[-2]  # returns the second last element of the list
# MAGIC     
# MAGIC and so on...

# COMMAND ----------

# Extend your list with new_list and print it
new_list = ["We", "are", "the", "knights", "who", "say", "Ni", "!"]
bar += new_list
print(bar)

# COMMAND ----------

# Replace "Ni" by "Ekke Ekke Ekke Ekke Ptang Zoo Boing" in the list and print it
bar[-2] = 'Ekke Ekke Ekke Ekke Ptang Zoo Boing'
print(bar)

# COMMAND ----------

# Compute the length of the list and print it
n= len(bar)
print(n)

# COMMAND ----------

# MAGIC %md ### What is the difference between lists and tuples?
# MAGIC Tuples are immutable, which is not the case for lists

# COMMAND ----------

# Create a dictionary containing the following mapping:
# "one" : 1
# "two" : 2
# etc. until you reach "five" : 5
baz = {'one' : 1, 'two' : 2, 'three' : 3, 'four' : 4, 'five' : 5}

# COMMAND ----------

# Check if the key "four" is contained in the dict
# If four is contained in the dict, print the associated value
if 'four' in baz : print(baz['four'])

# COMMAND ----------

gibberish = list("fqfgsrhrfeqluihjgrshioprqoqeionfvnorfiqeo")
# Find all the unique letters contained in gibberish. Your answer should fit in one line of code
unique_letters = [item for item in gibberish if gibberish.count(item)==1]
print(unique_letters)
# Test if your answer is correct:
# Test.assertEqualsHashed(unique_letters, 'b024fb7ad696c7d948d457a18499e56fc2fffcfa', "unique letters")

# COMMAND ----------

# MAGIC %md You should now be able to answer the following problem using dictionaries, lists and sets. Imagine you owe money to your friends because your forgot your credit card last time you went out for drinks. You want to remember how much you owe to each of them in order to refund them later. Which data structure would be useful to store this information? Use this data structure and fill it in with some debt data in the cell below:

# COMMAND ----------

debts = {'Tom' : -8, 'Richard' : -2}
debts

# COMMAND ----------

# MAGIC %md Another party night with more people, yet you forgot your credit card again... You meet new friends who buy you drinks. Create the same data structure as above with different data, i.e. include friends that were not here during the first party.

# COMMAND ----------

debts_2 = {'Tom' : -4, 'Anna' : -16, 'Paul' : -4}

# COMMAND ----------

# MAGIC %md Count the number of new friends you made that second night. Print the name of the friends who bought you drinks during the second party, but not during the first.

# COMMAND ----------

new_friends = [item for item in debts_2.keys() if item not in debts.keys()]
nb_new_friends = len(new_friends) # should fit in one line
print(nb_new_friends)

# COMMAND ----------

# MAGIC %md ## 3. Control flow
# MAGIC Same as before, read the corresponding section, and answer the questions below.
# MAGIC You can skip the paragraph on exceptions for now.

# COMMAND ----------

# Code the following:
# if you have made more than 5 friends that second night, 
# print "Yay! I'm super popular!", else, print "Duh..."
if nb_new_friends > 5 :
    print("Yay! I'm super popular!")
else : 
    print("Duh...")

# COMMAND ----------

# Now, thank each new friend iteratively, i.e.
# print "Thanks <name of the friend>!" using loops and string formatting (cf. section 1)
for name in new_friends : print "Thanks",name # This does not work the same for Python 3

# COMMAND ----------

# Sum all the number from 0 to 15 (included) using what we've seen so far (i.e. without the function sum() )
sum_to_fifteen = 0
for i in range(15) : sum_to_fifteen+=i+1

Test.assertEquals(sum_to_fifteen, 120)

# COMMAND ----------

# Note: you can break a loop with the break statement
for i in range(136):
    print(i)
    if i >= 2:
        break

# COMMAND ----------

# enumerate function can be very useful when dealing with iterators:
for i, value in enumerate(["a", "b", "c"]):
    print(value, i)

# COMMAND ----------

# MAGIC %md ## 4. Functions
# MAGIC Things are becoming more interesting. Read section 4. It's ok if you don't get the args/kwargs part. Be sure to understand basic function declaration and anonymous function declaration. Higher order functions, maps, and filters will be covered during the next lab.
# MAGIC 
# MAGIC Write a Python function that checks whether a passed string is palindrome or not. Note: a palindrome is a word, phrase, or sequence that reads the same backward and forward, e.g. "madam" or "nurses run". Hint: strings are lists of characters e.g.
# MAGIC 
# MAGIC     a = "abcdef"
# MAGIC     a[2] => c
# MAGIC     
# MAGIC If needed, here are [some tips about string manipulation](http://www.pythonforbeginners.com/basics/string-manipulation-in-python).

# COMMAND ----------

def isPalindrome(string_input):
    string_input = string_input.replace(" ", "")
    n = len(string_input)
    value = True
    for i in range(n) :
        if string_input[i] != string_input[n-1-i] : value = False
        if value == False or i>= n-1-i:
            break
    return value

Test.assertEquals(isPalindrome('aza'), True, "Simple palindrome") 
Test.assertEquals(isPalindrome('nurses run'), True, "Palindrome containing a space") 
Test.assertEquals(isPalindrome('palindrome'), False, "Not a palindrome") 

# COMMAND ----------

isPalindrome("nurses run")

# COMMAND ----------

# MAGIC %md Write a Python function to check whether a string is pangram or not. Note: pangrams are words or sentences containing every letter of the alphabet at least once. For example: "The quick brown fox jumps over the lazy dog".
# MAGIC 
# MAGIC [Hint](https://docs.python.org/2/library/stdtypes.html#set-types-set-frozenset)

# COMMAND ----------

import string

# In this function, "alphabet" argument has a default value: string.ascii_lowercase
# string.ascii_lowercase contains all the letters in lowercase.
def ispangram(string_input, alphabet=string.ascii_lowercase): 
    n = len(string_input)
    for i in range(n) :
        alphabet = alphabet.replace(string_input[i], "")
    if len(alphabet)==0 : return True
    else : return False
        
Test.assertEquals(ispangram('The quick brown fox jumps over the lazy dog'), True, "Pangram")
Test.assertEquals(ispangram('The quick red fox jumps over the lazy dog'), False, "Pangram")   

# COMMAND ----------

# MAGIC %md ### Python lambda expressions
# MAGIC 
# MAGIC When evaluated, lambda expressions return an anonymous function, i.e. a function that is not bound to any variable (hence the "anonymous"). However, it is possible to assign the function to a variable. Lambda expressions are particularly useful when you need to pass a simple function into another function. To create lambda functions, we use the following syntax
# MAGIC 
# MAGIC     lambda argument1, argument2, argument3, etc. : body_of_the_function
# MAGIC 
# MAGIC For example, a function which takes a number and returns its square would be
# MAGIC 
# MAGIC     lambda x: x**2
# MAGIC     
# MAGIC A function that takes two numbers and returns their sum:
# MAGIC 
# MAGIC     lambda x, y: x + y
# MAGIC     
# MAGIC `lambda` generates a function and returns it, while `def` generates a function and assigns it to a name.  The function returned by `lambda` also automatically returns the value of its expression statement, which reduces the amount of code that needs to be written.
# MAGIC 
# MAGIC Here are some additional references that explain lambdas: [Lambda Functions](http://www.secnetix.de/olli/Python/lambda_functions.hawk), [Lambda Tutorial](https://pythonconquerstheuniverse.wordpress.com/2011/08/29/lambda_tutorial/), and [Python Functions](http://www.bogotobogo.com/python/python_functions_lambda.php).
# MAGIC 
# MAGIC Here is an example:

# COMMAND ----------

# Function declaration using def
def add_s(x):
    return x + 's'

print type(add_s)
print add_s
print add_s('dog')

# COMMAND ----------

# Same function declared as a lambda
add_s_lambda = lambda x: x + 's'
print type(add_s_lambda)
print add_s_lambda  # Note that the function shows its name as <lambda>
print add_s_lambda('dog')

# COMMAND ----------

# Code a function using a lambda expression which takes
# a number and returns this number multiplied by two.
multiply_by_two = lambda x: x*2
print multiply_by_two(5)

Test.assertEquals(multiply_by_two(10), 20, 'incorrect definition for multiply_by_two')

# COMMAND ----------

# MAGIC %md Observe the behavior of the following code:

# COMMAND ----------

def add(x, y):
    """Add two values"""
    return x + y

def sub(x, y):
    """Substract y from x"""
    return x - y

functions = [add, sub]
print functions[0](1, 2)
print functions[1](3, 4)

# COMMAND ----------

# MAGIC %md Code the same functionality, using lambda expressions:

# COMMAND ----------

lambda_functions = [lambda x,y: x+y ,  lambda x, y : x-y]

Test.assertEquals(lambda_functions[0](1, 2), 3, 'add lambda_function')
Test.assertEquals(lambda_functions[1](3, 4), -1, 'sub lambda_function')

# COMMAND ----------

# MAGIC %md Lambda expressions can be used to generate functions that take in zero or more parameters. The syntax for `lambda` allows for multiple ways to define the same function.  For example, we might want to create a function that takes in a single parameter, where the parameter is a tuple consisting of two values, and the function adds the two values.  The syntax could be either 
# MAGIC 
# MAGIC     lambda x: x[0] + x[1]
# MAGIC     
# MAGIC or 
# MAGIC     
# MAGIC     lambda (x, y): x + y
# MAGIC 
# MAGIC If we called either function on the tuple `(1, 2)` it would return `3`.

# COMMAND ----------

# Example:
add_two_1 = lambda x, y: (x[0] + y[0], x[1] + y[1])
add_two_2 = lambda (x0, x1), (y0, y1): (x0 + y0, x1 + y1)
print 'add_two_1((1,2), (3,4)) = {0}'.format(add_two_1((1,2), (3,4)))
print 'add_two_2((1,2), (3,4)) = {0}'.format(add_two_2((1,2), (3,4)))

# COMMAND ----------

# Use both syntaxes to create a function that takes in a tuple of three values and reverses their order
# E.g. (1, 2, 3) => (3, 2, 1)
reverse1 = lambda x: (x[2], x[1], x[0])
reverse2 = lambda (x0, x1, x2): (x2, x1, x0)

Test.assertEquals(reverse1((1, 2, 3)), (3, 2, 1), 'reverse order, syntax 1')
Test.assertEquals(reverse2((1, 2, 3)), (3, 2, 1), 'reverse order, syntax 2')

# COMMAND ----------

# MAGIC %md Lambda expressions allow you to reduce the size of your code, but they are limited to simple logic. The following Python keywords refer to statements that cannot be used in a lambda expression: `assert`, `pass`, `del`, `print`, `return`, `yield`, `raise`, `break`, `continue`, `import`, `global`, and `exec`.  Assignment statements (`=`) and augmented assignment statements (e.g. `+=`) cannot be used either. If more complex logic is necessary, use `def` in place of `lambda`.

# COMMAND ----------

# MAGIC %md ## 5. Classes
# MAGIC Classes allow you to create objects. Object Oriented Programming (OOP) can be a very powerful paradigm. If done well, OOP  allows you to improve the modularity and reusability of your code, but that's the subject of an entire other course. 
# MAGIC Here is a *very* short introduction to it.
# MAGIC 
# MAGIC By convention, class names are written in camel case, e.g. `MyBeautifulClass`, while variable and function names are written in snake case, e.g. `my_variable`, `my_very_complex_function`
# MAGIC 
# MAGIC Classes contain methods (i.e. functions owned by the class) and attributes (i.e. variables owned by the class). 
# MAGIC When you define a class, first thing to do is to define a specific method, the constructor. In Python, the constructor is called `__init__`. This method is used to create the instances of an object. Example:
# MAGIC 
# MAGIC     class MyClass:
# MAGIC     
# MAGIC         def __init__(self, first_attribute, second_attribute):
# MAGIC             self.first_attribute = first_attribute
# MAGIC             self.second_attribute = second_attribute
# MAGIC             
# MAGIC This class has two attributes, and one (hidden) method, the constructor. To create an instance of this class, one simply does:
# MAGIC 
# MAGIC     instance_example = MyClass(1, "foo")
# MAGIC     
# MAGIC Then, the attributes can easily be accessed to:
# MAGIC 
# MAGIC     instance_example.first_attribute  # => 1
# MAGIC     instance_example.first_attribute  # => "foo"

# COMMAND ----------

# Run this example
class MyClass:
    
    def __init__(self, first_attribute, second_attribute):
        self.first_attribute = first_attribute
        self.second_attribute = second_attribute
            
instance_example = MyClass(1, "foo") 
print(instance_example.first_attribute)
instance_example.__init__(3,4)  # In real life, it is rare to reinit an object.
print(instance_example.first_attribute)

# COMMAND ----------

# MAGIC %md `self` denotes the object itself. When you declare a method, you have to pass `self` as the first argument of the method:
# MAGIC 
# MAGIC class MyClass:
# MAGIC     
# MAGIC     def __init__(self, first_attribute, second_attribute):
# MAGIC         self.first_attribute = first_attribute
# MAGIC         self.second_attribute = second_attribute
# MAGIC    
# MAGIC     def method_baz(self):
# MAGIC         print "Hello! I'm a method! I have two attributes, initialized with values %s, %s"%(self.first_attribute, self.second_attribute)
# MAGIC         
# MAGIC indeed, when we call
# MAGIC     
# MAGIC     instance_example = MyClass(1, "foo") 
# MAGIC     instance_example.method_baz()
# MAGIC     
# MAGIC the `self` object is implicitely passed to `method_baz`as an argument. Think of the method call as the following function call
# MAGIC 
# MAGIC     method_baz(instance_example)

# COMMAND ----------

# Run this example
class MyClass:
    
    def __init__(self, first_attribute, second_attribute):
        self.first_attribute = first_attribute
        self.second_attribute = second_attribute
    
    def class_method(self):
        print("Hello! I'm a method! My class has two attributes, of value {0}, {1}".format(self.first_attribute, self.second_attribute))
            
instance_example = MyClass(1, "foo") 
# Call to a class method
instance_example.class_method()

# COMMAND ----------

# MAGIC %md Now, the tricky part. You can declare **static** methods, i.e. methods that don't need to access the data contained in `self` to work properly. If you are a good observer, I made several calls of
# MAGIC 
# MAGIC     Test.assertEquals()
# MAGIC     
# MAGIC since the beginning of this lab. Test is a class, that has not even been instanciated, and `assertEquals` its method. This method is static: it doesn't require anything from the class `Test` constructor, thus can be used without instanciating the class. Such methods do not require the `self` argument as they do not use any instance data. They are implemented in the following way:

# COMMAND ----------

# Run this example
class MyClass:
    
    def __init__(self, first_attribute, second_attribute):
        self.first_attribute = first_attribute
        self.second_attribute = second_attribute
    
    def class_method(self):
        print("Hello! I'm a method! My class has two attributes, of value {0}, {1}".format(self.first_attribute, self.second_attribute))
         
    @staticmethod
    def static_method():
        print("I'm a static method!")
            
instance_example = MyClass(1, "foo") 
# Call to a class method
instance_example.class_method()
# Call to a static method
instance_example.static_method()

# COMMAND ----------

# Call to a static method without class instanciation
MyClass.static_method()

# COMMAND ----------

# Call to a class method without class instanciation: raises an error
# MyClass.class_method()
# => TypeError: unbound method class_method() must be called with MyClass instance as first argument (got nothing instead)

# COMMAND ----------

# MAGIC %md You can set attributes without passing them to the constructor:

# COMMAND ----------

# Run this example
class MyClass:
    
    default_attribute = 42
    
    def __init__(self, first_attribute, second_attribute):
        self.first_attribute = first_attribute
        self.second_attribute = second_attribute
    
    def method_baz(self):
        print("Hello! I'm a method! I have two attributes, initialized with values %s, %s"%(self.first_attribute, self.second_attribute))
        
    @staticmethod
    def static_method():
        print("I'm a static method!")
            
instance_example = MyClass(1, "foo") 
print(instance_example.default_attribute)

# COMMAND ----------

# Write a Python class named Rectangle which is 
# constructed by a length and width 
# and has two class methods
# - "rectange_area", which computes the area of a rectangle.
# - "rectangle_perimeter", which computes the perimeter of a rectangle.
#
# The Rectangle class should have an attribute n_edges equal to 4
# which should not be initialized by the __init__ constructor.
#
# Declare a static method "talk" that returns "Do you like rectangles?" when called

class Rectangle: 
    
    n_edges = 4
    
    def __init__(self, length, width):
        self.length = length
        self.width = width
    
    def rectangle_area(self):
        return self.length*self.width
    
    def rectangle_perimeter(self):
        return 2*self.length+2*self.width
    
    @staticmethod
    def talk():
        return "Do you like rectangles?"


new_rectangle = Rectangle(12, 10)
Test.assertEquals(new_rectangle.rectangle_area(), 120, "rectangle_area method")
Test.assertEquals(new_rectangle.rectangle_perimeter(), 44, "rectangle_area method")
Test.assertEquals(Rectangle.n_edges, 4, "constant attibute")
Test.assertEquals(Rectangle.talk(), "Do you like rectangles?", "Rectangle talk static method")

# COMMAND ----------

# MAGIC %md Congratulations, you've reched the end of this notebook. =)
