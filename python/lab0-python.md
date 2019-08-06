### Lab 0: Python Fundamentals

The purpose of this lab is to make sure you are sufficiently acquainted with Python to succeed in the rest of the labs. If Python is one of your primary language, this should be smooth sailing; otherwise, please make sure you complete these tasks before moving on to the next labs.

This lab assumes that you run with Python 3 on Databricks(Default option).

___

#### Task 1: Experimenting with the Python on Databricks

Open a python notebook on Databricks.  

> Run the code inside the cell using Ctrl + Enter
> Shift + Enter will run the code inside the cell and open a new cell

Try some of the following commands:

```
2 + 2
Out[5]: 4

print("Hello, Databricks")
Hello, Databricks
```

___

#### Task 2: Implementing Python Functions

You can define function using the ```def``` keyword:

```python
def run(name):
    print("Hey " + name)

run("databricks")
```

You should see "Hey databricks" printed out.

Next, implement a function called `wordcount` that takes a list of strings, and produces a dict with the number of times each string appears. Here is an example of its invocation and expected output:

```python
print(wordcount(["the", "fox", "jumped", "over", "the", "dog"]))
# Expecting { 'the': 2, 'fox': 1 }, and so on
```

You might find dict's `setdefault` method useful. To find out how it works, run `help(dict.setdefault)` from the notebook. Alternatively, to test whether a key is present in a dictionary, use `if key in dict ...`.

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
