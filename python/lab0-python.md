### Lab 0: Python Fundamentals

The purpose of this lab is to make sure you are sufficiently acquainted with Python to succeed in the rest of the labs. If Python is one of your primary language, this should be smooth sailing; otherwise, please make sure you complete these tasks before moving on to the next labs.

This lab assumes that you have Python 3.6+ installed on your system. If you're using the instructor-provided appliance, you're all set. Otherwise, please make sure that Python is installed and is in the path, so you can type `python` to launch it from a terminal window.

___

#### Task 1: Experimenting with the Python REPL

Open a terminal window and run `python`. An interactive prompt similar to the following should appear:

```
Python 3.6.0 |Anaconda 4.3.1 (64-bit)| (default, Dec 23 2016, 12:22:00) 
[GCC 4.4.7 20120313 (Red Hat 4.4.7-1)] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> 
```

This is the Python REPL -- Read, Eval, Print Loop environment. Try some basic commands to make sure everything works (do not type the `>>>` prompt):

```
>>> 2 + 2
4
>>> print("Hello, REPL")
Hello, REPL
>>> exit()
```

Instead of `exit()`, you can also type Ctrl+D to leave the REPL environment.

___

#### Task 2: Implementing Python Functions

Create a new file called `functions.py`. Use the following template so that when the file is executed directly, the `run` function will be called:

```python
def run():
    print("Hey there!")

if __name__ == "__main__":
    run()
```

> Which editor should you use in the appliance? If you want to get into the spirit of the course, you could use `vim`, but if you're looking for something more user-friendly, use `nano` or the built-in web-based editor.

To make sure everything's fine so far, run your Python program from a terminal window:

```
python functions.py
```

You should see "Hey there!" printed out.

Next, implement a function called `wordcount` that takes a list of strings, and produces a dict with the number of times each string appears. Here is an example of its invocation and expected output:

```python
print(wordcount(["the", "fox", "jumped", "over", "the", "dog"]))
# Expecting { 'the': 2, 'fox': 1 }, and so on
```

You might find dict's `setdefault` method useful. To find out how it works, run `help(dict.setdefault)` from the Python REPL. Alternatively, to test whether a key is present in a dictionary, use `if key in dict ...`.

**Solution**:

```python
def wordcount(words):
    freqs = {}
    for word in words:
        freqs[word] = freqs.setdefault(word, 0) + 1
    return freqs
```

___

#### Task 3: Using Collection Pipelines

Given a collection of items, the `map`, `filter`, `reduce` and other functions we learned are very useful for transforming the collection into your desired dataset. Implement the following functions according to the instructions provided, and do not use loops in your implementation:

* Given a list of numbers, use `filter` to filter out only the even numbers.

* Given a list of numbers, use `map` to raise each number to the power of 2.

* Given a list of words, use `reduce` to find the average word length(we need to import it from functools - `from functools import reduce`)

* Use `map` and `reduce` to solve [problem 6](https://projecteuler.net/problem=6) from Project Euler, which states:

> Find the difference between the sum of the squares of the first one hundred natural numbers and the square of the sum.

**Solution**:

```python
from functools import reduce
def evens(numbers):
    return filter(lambda n: n % 2 == 0, numbers)

def squares(numbers):
    return map(lambda n: n * n, numbers)

def avg_length(words):
    return reduce(lambda sum, word: sum + len(word), words, 0) / \
           float(len(words))

def problem6():
    def _sum(numbers):
        return reduce(lambda a, b: a + b, numbers)      # or use built-in sum()
    def square(n):
        return n * n
    return _sum(squares(range(1, 100))) - square(_sum(range(1, 100)))
```

___

#### Discussion

Why do you think Python is so successful in the data science, data analysis, machine learning, and scientific computing fields?

Compare the solutions above to your favorite programming language (or at least the one you're using in your day job). Do you feel the lack of strong typing makes Python code harder to read or write?
