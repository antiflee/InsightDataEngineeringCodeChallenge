import datetime
from math import ceil
from collections import defaultdict
from heapq import heappush, heappop

class MedianList:
    """
        This class is for medianvals_by_zip. To efficiently obtain the median
        from a data stream, we need two heaps, one stores the larger half portion 
        of the data, the other one stores the rest. We need to maintain the two
        heaps so that the high_heap is either equal to or one number longer
        than low_heap.
        
        The time complexity to add a new number is O(logn), and to obtain the
        median is O(1).
        
        Note here low_heap stores the "negative" value of the numbers, so that
        -heappop(low_heap) can directly give us the largest number in low_heap.
    """
    def __init__(self):
        """
            Initialize with two heaps, 
            high_heap stores larger half of the list,
            low_heap stores the other half.
        """
        self.high_heap = []
        self.low_heap = []
        self.total_amt = 0

    def addNumber(self, n):
        """
            Add a number into the MedianList.
        """
        heappush(self.high_heap, n)
        heappush(self.low_heap, -heappop(self.high_heap))
        if len(self.high_heap) < len(self.low_heap):
            heappush(self.high_heap, -heappop(self.low_heap))
        self.total_amt += n

    def getMedian(self):
        """
            Returns the median of data stream
        """
        if len(self.high_heap) > len(self.low_heap):
            return self.high_heap[0]
        return (self.high_heap[0]-self.low_heap[0]) / 2
    
    def getTotalNumOfTrans(self):
        """
            Returns total number of transactions.
        """
        return len(self.high_heap) + len(self.low_heap)
    
    def getTotalAmtOfTrans(self):
        """
            Returns total amount of transactions.
        """
        return self.total_amt

def process(line, recipient_zip_pair, recipient_date_pair, output_file_path_by_zip):
    """
        input: string
        return: void
        
        Process one line of data:
        1. Validate the data.
        2. Extract CMTE_ID, ZIP_CODE, TRANSACTION_DT and TRANSACTION_AMT.
        3. Write a line to the output_file_path_by_zip, with:
            CMTE_ID, ZIP_CODE, running_median, total_number_of_transaction,  total_amount_of_transaction.
        4. Save CMTE_ID, date and amount to a dictionary recipient_date_pair
    """
    if not line:
        return
        
    raw_data = line.split('|')
    
    if len(raw_data) <= 15:
        # print("Invalid data.\n\t"+line)
        return
    
    if raw_data[15]:
        # Only parse the data where OTHER_ID is null
        # print("Not donated by individuals, by {} instead.".format(raw_data[15]))
        return

    # Extract 'CMTE_ID','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT'
    data = [raw_data[i] for i in (0,10,13,14)]
    
    # If CMTE_ID or TRANSACTION_AMT is empty, don't parse the data.
    if not data[0] or not data[3]:
        return
        
    # Try to convert TRANSACTION_AMT to Float
    try:
        data[3] = float(data[3])
    except Exception as e:
        # print(e)
        return
    
    # Extract the first 5 digits of the zipcode
    # Invalid zipcode will be encoded as empty string
    data[1] = data[1][:5] if len(data[1]) >= 5 else ''
    
    if data[1]:
        # If the zipcode is valid, deal with medianvals_by_zip
    
        # First update the medianList, key = (CMTE_ID, Zipcode)
        medianList = recipient_zip_pair[(data[0],data[1])]
        
        medianList.addNumber(data[3])           # Insert the new donation
        
        newMedian = medianList.getMedian()
        newMedian = customRound(newMedian)      # Round the median using customRound()
        
        numOfTrans = medianList.getTotalNumOfTrans()
        amtOfTrans = medianList.getTotalAmtOfTrans()
        if int(amtOfTrans) == amtOfTrans:
            amtOfTrans = int(amtOfTrans)
        
        # write a line to medianvals_by_zip
        with open(output_file_path_by_zip, 'a') as output_file_zip:
            output_file_zip.write("|".join(
                                            [data[0], data[1], str(newMedian),
                                            str(numOfTrans), str(amtOfTrans)]              
                                        )+"\n")
    
    if is_valid_date(data[2]):
        # If the date is valid, first convert the format from
        # MMDDYYYY to YYYYMMDD
        data[2] = data[2][4:] + data[2][:4]
        # save the data to recipient_date_pair
        # We will deal with it at the end of the program.
        recipient_date_pair[(data[0],data[2])].append(data[3])
    
def is_valid_date(s):
    """
        Returns True if s is a valid MMDDYYYY format, False otherwise
    """
    if len(s) != 8:
        return False
    
    try:
        datetime.datetime.strptime(s, '%m%d%Y')
    except Exception as e:
        # print("{} is not a valid date format, should be MMDDYYYY".format(s))
        return False
    return True

def customRound(num):
    """
        Python by default performs bankers rounding, i.e., round half to even.
        But the code challenge requires always round 0.5 to 1.
        So we implement customRound function.
    """
    result = ceil(num) if (num%1) >= 0.5 else round(num)
    return int(result)

def main_func(input_file_path, output_file_path_by_zip, output_file_path_by_date):
    """
        Main function.
        1. Read the input file from input_file_path, line by line.
        2. For each line read, adopt the process() function to parse it.
        3. After recording all lines, analyze and write the result to medianvals_by_date.txt
    """
    # This dictionary stores all the donations to a (CMTE_ID, ZIPCODE) pair.
    # Key: (CMTE_ID, ZIPCODE), Value: MedianList
    recipient_zip_pair = defaultdict(MedianList)    

    # This dictionary stores all the donations to a (CMTE_ID, Date) pair.
    # Key: (CMTE_ID, DATE), Value: [transaction]
    recipient_date_pair = defaultdict(list)             
    
    
    with open(input_file_path, 'r', buffering=-1) as input_file:
        for line in input_file:
            process(line, recipient_zip_pair, recipient_date_pair, output_file_path_by_zip)
    
    # Write to medianvals_by_date.txt
    with open(output_file_path_by_date, 'w') as output_file_date:
        for key in sorted(recipient_date_pair.keys()):
            val = recipient_date_pair[key]
            if val:
                val.sort()
                
                recipient, date = key[0], key[1]
                # Convert date from YYYYMMDD back to MMDDYYYY
                date = date[4:] + date[:4]                
                
                total_num_of_tran, total_amt_of_tran = len(val), sum(val)
                if int(total_amt_of_tran) == total_amt_of_tran:
                    total_amt_of_tran = int(total_amt_of_tran)
                
                if len(val) % 2:
                    median = val[len(val)//2]
                else:
                    median = (val[len(val)//2-1] + val[len(val)//2])/2
                    
                median = customRound(median)    
                
                output_file_date.write("|".join(
                                                [recipient, date, str(median), 
                                                str(total_num_of_tran), str(total_amt_of_tran)]
                                        )+"\n")
                

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Not all three file paths found(1 input and 2 output)...\n")
        print("Using default file paths instead...\n")
        input_file_path = 'input\itcont.txt'
        output_file_path_by_zip = 'output\medianvals_by_zip.txt'
        output_file_path_by_date = 'output\medianvals_by_date.txt'
    else:
        input_file_path = sys.argv[1]
        output_file_path_by_zip = sys.argv[2]
        output_file_path_by_date = sys.argv[3]

    main_func(input_file_path, output_file_path_by_zip, output_file_path_by_date)
        









