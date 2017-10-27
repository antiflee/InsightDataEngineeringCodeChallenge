### This is my Python3 solution to the Insight Data Engineering Code Challenge, for the January 2018 session.
#### by Y. Liu

#### The description of the problem can be found at https://github.com/InsightDataScience/find-political-donors

#### Description of the solution:

0. The solution only uses Python3 standard libraries. The python3 script is `src/find_political_donors.py`. To execute, run `./run.sh`. No "exotic" packages needed.

1. Two dictionaries are used to store the information of donations when parsing the input file line by line.
    * *recipient_zip_pair*: the key is (recipient, zipcode), and the value is a MedianList instance (for the detail of the class, see below).
    * *recipient_date_pair*: the key is (recipient, date), and the value is a list that stores all corresponding transactions.

2. The input file is read line by line. Here *buffering* is set to *-1*, which is the system default value, when opening the input file.

3. For each line, first validate the data. If valid, write a line to the *medianvals_by_zip.txt* (if zipcode is valid), and stores the data to the dictionaries.

4. After recording all data, analyze and write to *medianvals_by_date.txt*.

5. The MedianList class is used to efficiently calculate the running median. Two heaps are maintained for each (recipient, zipcode) pair: One stores the larger half of transactions, and the other one stores the rest. This class has a *O(1)* complexity to get the median, and *O(logn)* complexity to add a transaction to it.
