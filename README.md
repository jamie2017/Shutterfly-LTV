#Shutterfly Customer Lifetime Value Code Challenge

### Author: Jianmei Ye
### 2017-05-01


## Problem Statement

Write a program that ingests event data and implements one analytic method, below. You are expected to write clean, well-documented and well-tested code. Be sure to think about performance - what the performance characteristic of the code is and how it could be improved in the future.

### Ingest(e, D)
Given event e, update data D

### TopXSimpleLTVCustomers(x, D)
Return the top x customers with the highest Simple Lifetime Value from data D. 


## Solutions

##1 Handle data and request in one python script
     Follow the problem requirement,
     write a program that can read input data from file, 
     parse data then return top X simple LTV Customer result to output file.
     - TopXSimpleLTV.py
     
##2 Handle data and request for long term use
    Apply same logic implementing Ingest(e,D) and TopXSimpleLTVCustomers(x,D) as stated,
    Build a Restful APIs [GET,POST] with MongoDb deploy on AWS EC2
     - requirements.txt
     - SimpleAPI.py
     - TopXSimpleLTVForApi.py
  
### Run example:
#### Generate output file
    #Top 5 results
    python TopXSimpleLTV.py 5
#### APIs:
    POST: Insert input data "data.json" to MongoDB 
        curl -H "Content-Type: application/json" --data @data.json http://54.218.115.137:5000/

    GET:
        http://54.218.115.137:5000/topxsimpleltv?x=3
    
     
     
## Directory structure

```
- README.md
- src 
- input 
- output 
- sample_input 
- requirements.txt
```

## Reference
https://blog.kissmetrics.com/how-to-calculate-lifetime-value
https://github.com/sflydwh/code-challenge




